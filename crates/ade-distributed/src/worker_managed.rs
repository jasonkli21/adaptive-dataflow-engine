//! Process-local live session fencing plus persistent deployment/high-water binding.
use super::{State, handle};
use crate::managed::{self, Answer, Boot, Fence, Message, Operation, Response};
use crate::{
    Error, Result,
    ids::JobId,
    protocol::{self, Request},
    require,
};
use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    sync::atomic::AtomicUsize,
};
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Watermark {
    version: u32,
    store: String,
    job: u64,
    owner: u64,
    checksum: String,
}
struct Guard<'a>(&'a AtomicUsize);
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
pub(super) struct Managed {
    pub boot: Boot,
    root: PathBuf,
    _lock: File,
    active: Mutex<Option<Fence>>,
    draining: AtomicBool,
    calls: AtomicUsize,
    disk: Mutex<Arc<crate::disk::Budget>>,
    watermark: Mutex<Watermark>,
    #[cfg(feature = "research-hooks")]
    transient_issued: AtomicBool,
}
impl Managed {
    pub fn open(parent: &Path, store: &str) -> Result<Self> {
        require(
            uuid::Uuid::parse_str(store).is_ok_and(|id| !id.is_nil()),
            "managed deployment UUID",
        )?;
        require(
            parent.is_absolute()
                && parent.canonicalize()? == parent
                && fs::metadata(parent)?.is_dir()
                && fs::metadata(parent)?.permissions().mode() & 0o777 == 0o700,
            "managed private canonical scratch",
        )?;
        for entry in parent.ancestors() {
            require(
                !fs::symlink_metadata(entry)?.file_type().is_symlink(),
                "managed scratch symlink",
            )?;
        }
        for name in ["managed.lock", "managed.json"] {
            match fs::symlink_metadata(parent.join(name)) {
                Ok(m) => require(
                    m.is_file() && m.nlink() == 1 && m.permissions().mode() & 0o777 == 0o600,
                    "unsafe managed worker metadata",
                )?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(parent.join("managed.lock"))?;
        require(
            lock.try_lock().is_ok(),
            "managed worker scratch already owned",
        )?;
        let path = parent.join("managed.json");
        let watermark = if path.exists() {
            require(
                fs::symlink_metadata(&path)?.is_file(),
                "managed watermark regular file",
            )?;
            let file = File::open(&path)?;
            require(file.metadata()?.len() <= 1024, "managed watermark cap")?;
            let mut bytes = vec![];
            file.take(1025).read_to_end(&mut bytes)?;
            let mark: Watermark = serde_json::from_slice(&bytes)?;
            require(
                mark.version == 1
                    && mark.store == store
                    && mark.checksum
                        == blake3::hash(&serde_json::to_vec(&(
                            mark.version,
                            &mark.store,
                            mark.job,
                            mark.owner,
                        ))?)
                        .to_hex()
                        .as_str(),
                "managed deployment binding/version/checksum",
            )?;
            mark
        } else {
            // Refuse a missing watermark in a previously used scratch directory.
            require(
                fs::read_dir(parent)?.all(|e| e.is_ok_and(|e| e.file_name() == "managed.lock")),
                "missing managed watermark in nonempty scratch",
            )?;
            Watermark {
                version: 1,
                store: store.into(),
                job: 0,
                owner: 0,
                checksum: String::new(),
            }
        };
        let managed = Self {
            boot: Boot {
                version: 1,
                store: store.into(),
                instance: uuid::Uuid::new_v4().to_string(),
                research_hooks: cfg!(feature = "research-hooks"),
                runtime: crate::managed::RUNTIME_ID.into(),
            },
            root: parent.into(),
            _lock: lock,
            active: Mutex::default(),
            draining: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
            disk: Mutex::new(crate::disk::Budget::new(managed::DISK_BYTES)),
            watermark: Mutex::new(watermark),
            #[cfg(feature = "research-hooks")]
            transient_issued: AtomicBool::new(false),
        };
        managed.save()?;
        Ok(managed)
    }
    fn save(&self) -> Result<()> {
        let next = self.root.join("managed.next");
        if next.try_exists()? {
            require(
                fs::symlink_metadata(&next)?.is_file(),
                "managed temp regular file",
            )?;
            fs::remove_file(&next)?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&next)?;
        let bytes = {
            let mut mark = self.watermark.lock().expect("watermark");
            mark.checksum = blake3::hash(&serde_json::to_vec(&(
                mark.version,
                &mark.store,
                mark.job,
                mark.owner,
            ))?)
            .to_hex()
            .to_string();
            serde_json::to_vec(&*mark)?
        };
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(next, self.root.join("managed.json"))?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
    pub fn budget(&self) -> Arc<crate::disk::Budget> {
        self.disk.lock().expect("disk quota").clone()
    }
    pub fn last_job(&self) -> Option<JobId> {
        let n = self.watermark.lock().expect("watermark").job;
        (n > 0).then_some(JobId(n))
    }
    fn check(&self, fence: &Fence) -> Result<()> {
        fence.validate()?;
        require(
            fence.boot == self.boot.instance
                && fence.store == self.boot.store
                && std::path::Path::new(&fence.scratch) == self.root,
            "managed worker boot/deployment mismatch",
        )
    }
    #[allow(
        clippy::too_many_lines,
        reason = "One bounded worker state machine keeps durable claim, gate closure and drain proof together."
    )]
    fn execute(&self, state: &State, message: Message) -> Result<Response> {
        self.check(&message.fence)?;
        match message.operation {
            Operation::Hello => Ok(Response::Hello(Box::new(managed::Ready {
                endpoint: state.endpoint.clone(),
                boot: self.boot.clone(),
            }))),
            Operation::Claim => {
                let mut active = self.active.lock().expect("managed fence");
                if active.as_ref() == Some(&message.fence) && !self.draining.load(Ordering::Acquire)
                {
                    return Ok(Response::Ready);
                }
                require(
                    active.is_none()
                        && self.calls.load(Ordering::Acquire) == 0
                        && !state.poisoned.load(Ordering::Acquire),
                    "old managed session not drained/worker poisoned",
                )?;
                {
                    let mut mark = self.watermark.lock().expect("watermark");
                    require(
                        message.fence.owner > mark.owner && message.fence.job > mark.job,
                        "stale managed owner/job",
                    )?;
                    mark.owner = message.fence.owner;
                    mark.job = message.fence.job;
                }
                // Fail closed after an ambiguous persistence error.
                state.poisoned.store(true, Ordering::Release);
                self.save()?;
                state.poisoned.store(false, Ordering::Release);
                *self.disk.lock().expect("disk quota") =
                    crate::disk::Budget::new(message.fence.disk_bytes);
                *active = Some(message.fence);
                self.draining.store(false, Ordering::Release);
                Ok(Response::Ready)
            }
            Operation::Drain => {
                {
                    let active = self.active.lock().expect("managed fence");
                    if active.is_none() {
                        require(
                            self.calls.load(Ordering::Acquire) == 0
                                && !state.poisoned.load(Ordering::Acquire),
                            "unproved worker quiescence",
                        )?;
                        return Ok(Response::Drained {
                            disk_bytes: self.budget().used(),
                        });
                    }
                    require(
                        active.as_ref() == Some(&message.fence),
                        "drain session mismatch",
                    )?;
                    // Same gate as Call's check-and-increment: no request can
                    // pass validation and increment after this fence closes.
                    self.draining.store(true, Ordering::Release);
                }
                let until = Instant::now() + Duration::from_secs(3);
                let budget = self.budget();
                loop {
                    handle(
                        state,
                        Request::CancelJob {
                            job: JobId(message.fence.job),
                        },
                    )?;
                    // Repeat cancellation until earlier Prepare/Commit calls
                    // have returned; they may have installed a session after
                    // the first cancellation. Every admitted store/writer and
                    // detached spool holds THIS shared budget ownership token.
                    if self.calls.load(Ordering::Acquire) == 0
                        && Arc::strong_count(&budget) == 2
                        && state.job.lock().expect("job lock").is_none()
                        && state.join.lock().expect("join lock").is_none()
                    {
                        break;
                    }
                    require(
                        Instant::now() < until,
                        "managed drain timeout; needs operator",
                    )?;
                    thread::sleep(Duration::from_millis(2));
                }
                require(
                    !state.poisoned.load(Ordering::Acquire),
                    "managed poisoned worker needs operator",
                )?;
                *self.active.lock().expect("managed fence") = None;
                Ok(Response::Drained {
                    disk_bytes: self.budget().used(),
                })
            }
            Operation::Call(request) => {
                {
                    let active = self.active.lock().expect("managed fence");
                    require(
                        active.as_ref() == Some(&message.fence)
                            && !self.draining.load(Ordering::Acquire),
                        "stale/draining managed session",
                    )?;
                    self.calls.fetch_add(1, Ordering::AcqRel);
                }
                let _guard = Guard(&self.calls);
                match request.as_ref() {
                    Request::PrepareJob(spec) => require(
                        spec.job.0 == message.fence.job
                            && spec.execution_namespace == message.fence.instance
                            && !spec.config.spill_enabled
                            && !spec.config.scheduler.leased()
                            && !spec.config.static_projected
                            && spec.config.partial.mode == ade_exec::partial::Mode::FixedHash
                            && spec.config.topology.policy
                                == crate::topology::PartitionPolicy::Static
                            && spec.config.exchange.mode == crate::ExchangeMode::Buffered
                            && spec.config.exchange.cost.is_none(),
                        "managed aggregate configuration/refusal",
                    )?,
                    Request::PrepareJoin(spec) => require(
                        spec.job.0 == message.fence.job
                            && spec.execution == message.fence.instance
                            && !spec.config.local.spill_enabled
                            && spec.config.local.algorithm == ade_exec::join::Algorithm::HashOnly,
                        "managed join hash-only/spill refusal",
                    )?,
                    Request::Shutdown
                    | Request::RecoverFinal { .. }
                    | Request::CleanupMaterializations { .. } => {
                        return Err(Error::Invalid("managed operation refused".into()));
                    }
                    _ => {}
                }
                #[cfg(feature = "research-hooks")]
                if matches!(
                    request.as_ref(),
                    Request::PrepareJob(_) | Request::PrepareJoin(_)
                ) && std::env::var("ADE_MANAGED_FAIL_ONCE").as_deref() == Ok("1")
                    && !self.transient_issued.swap(true, Ordering::AcqRel)
                {
                    eprintln!(
                        "ADE_MANAGED_REACHED transient_prepare job={}",
                        message.fence.job
                    );
                    return Err(Error::Remote {
                        code: "transient".into(),
                        message: "injected transient prepare failure".into(),
                    });
                }
                // Every job-bearing native request is additionally checked by
                // its existing native handler; no generic replay is added.
                handle(state, *request).map(|reply| Response::Reply(Box::new(reply)))
            }
        }
    }
    pub fn socket(&self, state: &State, socket: &mut std::net::TcpStream) {
        let message = managed::read_message::<Message>(&mut protocol::DeadlineStream::new(
            socket,
            crate::IO_TIMEOUT,
        ));
        if let Ok(message) = message {
            let fence = message.fence.clone();
            let response = self
                .execute(state, message)
                .unwrap_or_else(|e| Response::Error {
                    code: match &e {
                        Error::Remote { code, .. } => code.clone(),
                        _ => "managed".into(),
                    },
                    message: e.to_string().chars().take(1024).collect(),
                });
            let _ = managed::write_message(
                &mut protocol::DeadlineStream::new(socket, crate::IO_TIMEOUT),
                &Answer { fence, response },
            );
        }
    }
}
