//! Full content identity checks, with an explicit scoped CPU budget.
use super::{CancellationToken, Error, Instant, Result, SourceMorsel, require};
const CHUNK_BYTES: usize = 1024 * 1024;
const HASH_THREADS: usize = 8;

pub(crate) fn producer_threads(workers: usize) -> usize {
    (HASH_THREADS / workers).max(1)
}

/// Build no global state. `build_scoped` joins all hash threads before returning,
/// on both success and failure; at most eight threads/512KiB stacks per job stage.
pub(crate) fn with_verifier<T>(
    threads: usize,
    work: impl FnOnce(&Verifier<'_>) -> Result<T>,
) -> Result<T> {
    with_scratch(threads, CHUNK_BYTES, work)
}

pub(crate) fn with_catalog_verifier<T>(work: impl FnOnce(&Verifier<'_>) -> Result<T>) -> Result<T> {
    with_scratch(HASH_THREADS, 256 * 1024, work)
}

fn with_scratch<T>(
    threads: usize,
    chunk_bytes: usize,
    work: impl FnOnce(&Verifier<'_>) -> Result<T>,
) -> Result<T> {
    require(
        (1..=HASH_THREADS).contains(&threads),
        "source hash thread cap",
    )?;
    rayon_core::ThreadPoolBuilder::new()
        .num_threads(threads)
        .stack_size(512 * 1024)
        .thread_name(|i| format!("ade-source-hash-{i}"))
        .build_scoped(rayon_core::ThreadBuilder::run, |pool| {
            work(&Verifier {
                pool,
                resources: Resources::default(),
                scratch: std::sync::Mutex::new(Scratch::default()),
                chunk_bytes,
            })
        })
        .map_err(|e| Error::Invalid(format!("source hash pool: {e}")))?
}

pub(crate) struct Verifier<'a> {
    pool: &'a rayon_core::ThreadPool,
    resources: Resources,
    scratch: std::sync::Mutex<Scratch>,
    chunk_bytes: usize,
}

#[derive(Default)]
struct Scratch {
    available: Vec<Vec<u8>>,
    allocated: usize,
}

#[derive(Default)]
struct Resources {
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}
struct BufferGuard<'a, 'pool> {
    verifier: &'a Verifier<'pool>,
    bytes: Vec<u8>,
}
impl Drop for BufferGuard<'_, '_> {
    fn drop(&mut self) {
        let mut scratch = self.verifier.scratch.lock().expect("source scratch");
        self.verifier
            .resources
            .active
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        scratch.available.push(std::mem::take(&mut self.bytes));
    }
}

impl Verifier<'_> {
    pub(crate) fn threads(&self) -> usize {
        self.pool.current_num_threads()
    }
    pub(crate) fn scratch_peak(&self) -> usize {
        self.resources
            .peak
            .load(std::sync::atomic::Ordering::Acquire)
            * self.chunk_bytes
    }
    fn buffer(&self) -> Result<BufferGuard<'_, '_>> {
        let mut scratch = self
            .scratch
            .lock()
            .map_err(|_| Error::Invalid("source scratch poisoned".into()))?;
        let bytes = if let Some(bytes) = scratch.available.pop() {
            bytes
        } else {
            require(
                scratch.allocated < self.threads(),
                "source hash scratch/FD cap",
            )?;
            scratch.allocated += 1;
            self.resources
                .peak
                .fetch_max(scratch.allocated, std::sync::atomic::Ordering::AcqRel);
            vec![0; self.chunk_bytes]
        };
        self.resources
            .active
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(BufferGuard {
            verifier: self,
            bytes,
        })
    }
    pub(crate) fn verify(
        &self,
        morsels: &[SourceMorsel],
        token: &CancellationToken,
        deadline: Instant,
    ) -> Result<()> {
        require(morsels.len() <= 1024, "source verification catalog cap")?;
        if morsels.len() == 1 {
            let mut buffer = self.buffer()?;
            return self
                .pool
                .install(|| verify_file(&morsels[0], token, deadline, &mut buffer.bytes, true));
        }
        let failure = std::sync::Mutex::new(None);
        self.pool.scope(|scope| {
            // File-level work stealing balances asymmetric cores. No nested
            // hashing here: at most N running tasks hold N scratch buffers/FDs,
            // with at most the catalog's 1024 bounded jobs queued. Single-file
            // producer verification above still splits a file across its pool.
            for morsel in morsels {
                let failure = &failure;
                scope.spawn(move |_| {
                    let result = (|| {
                        let mut buffer = self.buffer()?;
                        verify_file(morsel, token, deadline, &mut buffer.bytes, false)
                    })();
                    if let Err(error) = result {
                        let mut first = failure.lock().expect("source verification failure");
                        if first.is_none() {
                            *first = Some(error);
                        }
                        token.cancel();
                    }
                });
            }
        });
        failure
            .into_inner()
            .map_err(|_| Error::Invalid("source verification poisoned".into()))?
            .map_or(Ok(()), Err)
    }
}

/// Serial reference for existing focused tests; production uses the owned pool.
#[cfg(test)]
pub(crate) fn verify_sources(
    morsels: &[SourceMorsel],
    token: &CancellationToken,
    deadline: Instant,
) -> Result<()> {
    let mut bytes = vec![0; CHUNK_BYTES];
    for morsel in morsels {
        verify_file(morsel, token, deadline, &mut bytes, false)?;
    }
    Ok(())
}

fn verify_file(
    morsel: &SourceMorsel,
    token: &CancellationToken,
    deadline: Instant,
    bytes: &mut [u8],
    parallel: bool,
) -> Result<()> {
    token.check()?;
    require(
        Instant::now() < deadline,
        "scheduler source verification deadline",
    )?;
    let mut file = std::fs::File::open(&morsel.path)?;
    require(
        file.metadata()?.len() == morsel.bytes,
        "source snapshot size mismatch",
    )?;
    // Scratch is reused without retaining source bytes as identity evidence.
    // Every pass reopens and reads the entire file and checks the current hash.
    verify_reader(&mut file, morsel, token, deadline, bytes, parallel)
}

fn verify_reader(
    file: &mut impl std::io::Read,
    morsel: &super::SourceMorsel,
    token: &CancellationToken,
    deadline: Instant,
    bytes: &mut [u8],
    parallel: bool,
) -> Result<()> {
    let mut digest = blake3::Hasher::new();
    let mut read = 0u64;
    loop {
        token.check()?;
        require(
            Instant::now() < deadline,
            "scheduler source verification deadline",
        )?;
        let n = file.read(bytes)?;
        if n == 0 {
            break;
        }
        read = read
            .checked_add(n as u64)
            .ok_or_else(|| Error::Invalid("source verification byte overflow".into()))?;
        require(read <= morsel.bytes, "source grew during verification")?;
        if parallel {
            digest.update_rayon(&bytes[..n]);
        } else {
            digest.update(&bytes[..n]);
        }
    }
    require(
        read == morsel.bytes && digest.finalize().to_hex().as_str() == morsel.fingerprint,
        "source snapshot fingerprint mismatch",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, time::Duration};
    struct InterruptRead {
        token: CancellationToken,
        expire: bool,
    }
    impl std::io::Read for InterruptRead {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            bytes.fill(42);
            if self.expire {
                thread::sleep(Duration::from_millis(3));
            } else {
                self.token.cancel();
            }
            Ok(bytes.len())
        }
    }
    #[test]
    fn source_pass_checks_cancellation_and_deadline_between_chunks() {
        for expire in [false, true] {
            let token = CancellationToken::default();
            let mut reader = InterruptRead {
                token: token.clone(),
                expire,
            };
            let morsel = super::super::SourceMorsel {
                id: crate::ids::MorselId(0),
                path: "/unused".into(),
                bytes: 1024 * 1024,
                fingerprint: "a".repeat(64),
            };
            let error = verify_reader(
                &mut reader,
                &morsel,
                &token,
                Instant::now() + Duration::from_millis(if expire { 1 } else { 1000 }),
                &mut vec![0; 256 * 1024],
                false,
            )
            .unwrap_err();
            if expire {
                assert!(error.to_string().contains("verification deadline"));
            } else {
                assert!(token.check().is_err());
            }
        }
    }
}

#[cfg(test)]
mod parallel_tests {
    use super::*;
    use crate::ids::MorselId;
    use std::time::Duration;

    fn fixture() -> (tempfile::TempDir, Vec<SourceMorsel>) {
        let root = tempfile::tempdir().unwrap();
        let sources = [
            0,
            1,
            8191,
            8192,
            16385,
            CHUNK_BYTES - 1,
            CHUNK_BYTES,
            CHUNK_BYTES + 7,
        ]
        .into_iter()
        .enumerate()
        .map(|(id, size)| {
            let bytes: Vec<_> = (0..size)
                .map(|i| u8::try_from(i % 251).expect("modulo byte"))
                .collect();
            let path = root.path().join(id.to_string());
            std::fs::write(&path, &bytes).unwrap();
            SourceMorsel {
                id: MorselId(id as u64),
                path,
                bytes: size as u64,
                fingerprint: blake3::hash(&bytes).to_hex().to_string(),
            }
        })
        .collect::<Vec<_>>();
        (root, sources)
    }
    #[test]
    fn scoped_parallel_hash_is_canonical_bounded_and_cancels_on_drift() {
        let (_root, sources) = fixture();
        for threads in [1, 2, 4] {
            with_verifier(threads, |verifier| {
                verifier.verify(
                    &sources,
                    &CancellationToken::default(),
                    Instant::now() + Duration::from_secs(5),
                )?;
                assert!(verifier.scratch_peak() <= threads * CHUNK_BYTES);
                assert_eq!(
                    verifier
                        .resources
                        .active
                        .load(std::sync::atomic::Ordering::Acquire),
                    0
                );
                for source in &sources {
                    verifier.verify(
                        std::slice::from_ref(source),
                        &CancellationToken::default(),
                        Instant::now() + Duration::from_secs(5),
                    )?;
                }
                // Reusing scratch must never reuse identity evidence. Replace
                // the already verified one-byte file without changing its size.
                std::fs::write(&sources[1].path, [42]).unwrap();
                assert!(
                    verifier
                        .verify(
                            std::slice::from_ref(&sources[1]),
                            &CancellationToken::default(),
                            Instant::now() + Duration::from_secs(5)
                        )
                        .is_err()
                );
                std::fs::write(&sources[1].path, [0]).unwrap();
                assert_eq!(
                    verifier
                        .resources
                        .active
                        .load(std::sync::atomic::Ordering::Acquire),
                    0
                );
                Ok(())
            })
            .unwrap();
        }
        with_catalog_verifier(|verifier| {
            verifier.verify(
                &sources,
                &CancellationToken::default(),
                Instant::now() + Duration::from_secs(5),
            )?;
            assert_eq!(verifier.threads(), 8);
            assert!(verifier.scratch_peak() <= 8 * 256 * 1024);
            let scratch = verifier.scratch.lock().unwrap();
            assert_eq!(scratch.allocated, scratch.available.len());
            assert_eq!(
                verifier
                    .resources
                    .active
                    .load(std::sync::atomic::Ordering::Acquire),
                0
            );
            Ok(())
        })
        .unwrap();
        std::fs::write(&sources[7].path, vec![42; CHUNK_BYTES + 7]).unwrap();
        let token = CancellationToken::default();
        assert!(
            with_verifier(4, |verifier| {
                let result =
                    verifier.verify(&sources, &token, Instant::now() + Duration::from_secs(5));
                assert!(verifier.scratch_peak() <= 4 * CHUNK_BYTES);
                assert_eq!(
                    verifier
                        .resources
                        .active
                        .load(std::sync::atomic::Ordering::Acquire),
                    0
                );
                result
            })
            .is_err()
        );
        assert!(token.check().is_err());
        for threads in [0, 9] {
            assert!(with_verifier(threads, |_| Ok(())).is_err());
        }
    }
}
