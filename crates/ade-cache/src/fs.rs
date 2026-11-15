use crate::{Control, Error, Result};
use rustix::fs::{AtFlags, Dir, Mode, OFlags, open, openat};
use std::{
    fs::{File, Metadata, TryLockError},
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Component, Path},
    time::{Duration, Instant},
};

pub(crate) fn uid() -> u32 {
    rustix::process::geteuid().as_raw()
}

const OWNER_DIR: Mode = Mode::from_bits_truncate(0o700);
const OWNER_FILE: Mode = Mode::from_bits_truncate(0o600);
const OPEN_FLAGS: OFlags = OFlags::NOFOLLOW
    .union(OFlags::NONBLOCK)
    .union(OFlags::CLOEXEC);

pub(crate) fn directory(path: &Path, private: bool) -> Result<File> {
    if !path.is_absolute() || path.as_os_str().len() > 4096 {
        return Err(Error::Access);
    }
    let mut dir = File::from(open("/", OPEN_FLAGS | OFlags::DIRECTORY, Mode::empty())?);
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                dir = open_dir(&dir, Path::new(name)).map_err(|_| Error::Access)?;
                let m = dir.metadata()?;
                let sticky_system_root = m.uid() == 0 && m.mode() & 0o1000 != 0;
                if (m.uid() != uid() && m.uid() != 0)
                    || (m.mode() & 0o022 != 0 && !sticky_system_root)
                {
                    return Err(Error::Access);
                }
            }
            _ => return Err(Error::Access),
        }
    }
    if private {
        check_dir(&dir)?;
    }
    Ok(dir)
}

pub(crate) fn open_dir(parent: &File, name: &Path) -> Result<File> {
    Ok(openat(parent, name, OPEN_FLAGS | OFlags::DIRECTORY, Mode::empty())?.into())
}

pub(crate) fn private_dir(parent: &File, name: &str) -> Result<File> {
    let dir = open_dir(parent, Path::new(name))?;
    check_dir(&dir)?;
    if dir.metadata()?.dev() != parent.metadata()?.dev() {
        return Err(Error::Access);
    }
    Ok(dir)
}

fn check_dir(dir: &File) -> Result<()> {
    let m = dir.metadata()?;
    if !m.is_dir() || m.uid() != uid() || m.mode() & 0o7777 != 0o700 {
        return Err(Error::Access);
    }
    Ok(())
}

pub(crate) fn create_dir(parent: &File, name: &str) -> Result<File> {
    rustix::fs::mkdirat(parent, name, OWNER_DIR)?;
    private_dir(parent, name)
}

pub(crate) fn data(parent: &File, name: &str, flags: OFlags, private: bool) -> Result<File> {
    let file = File::from(openat(parent, name, OPEN_FLAGS | flags, OWNER_FILE)?);
    let m = file.metadata()?;
    if !m.is_file() || m.nlink() != 1 || (m.uid() != uid() && (private || m.uid() != 0)) {
        return Err(Error::Access);
    }
    if (private && m.mode() & 0o7777 != 0o600) || (!private && m.mode() & 0o022 != 0) {
        return Err(Error::Access);
    }
    Ok(file)
}

pub(crate) fn new_data(parent: &File, name: &str) -> Result<File> {
    data(
        parent,
        name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL,
        true,
    )
}

pub(crate) fn names(dir: &File, cap: usize, control: &Control) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for item in Dir::read_from(dir)? {
        control.check()?;
        let item = item?;
        let name = item.file_name().to_str().map_err(|_| Error::Access)?;
        if matches!(name, "." | "..") {
            continue;
        }
        if names.len() == cap {
            return Err(Error::Integrity("directory inventory cap"));
        }
        names.push(name.to_owned());
    }
    names.sort_unstable();
    Ok(names)
}

pub(crate) fn move_dir(parent: &File, from: &str, to: &str) -> Result<()> {
    rustix::fs::renameat_with(parent, from, parent, to, rustix::fs::RenameFlags::NOREPLACE)?;
    Ok(())
}

pub(crate) fn unlink(parent: &File, name: &str, dir: bool) -> Result<()> {
    rustix::fs::unlinkat(
        parent,
        name,
        if dir {
            AtFlags::REMOVEDIR
        } else {
            AtFlags::empty()
        },
    )?;
    Ok(())
}

pub(crate) fn read_bounded(mut file: File, cap: usize, control: &Control) -> Result<Vec<u8>> {
    if file.metadata()?.len() > cap as u64 {
        return Err(Error::Capacity);
    }
    let mut output = Vec::new();
    let mut buf = [0; 8192];
    loop {
        control.check()?;
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        if n > cap - output.len() {
            return Err(Error::Capacity);
        }
        output.extend_from_slice(&buf[..n]);
    }
    Ok(output)
}

pub(crate) fn digest(mut file: &File, bytes: u64, control: &Control) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0; 64 * 1024];
    let mut remaining = bytes;
    loop {
        control.check()?;
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        remaining = remaining
            .checked_sub(n as u64)
            .ok_or(Error::Integrity("file grew"))?;
        hasher.update(&buf[..n]);
    }
    if remaining != 0 {
        return Err(Error::Integrity("file shrank"));
    }
    Ok(hasher.finalize().to_hex().to_string())
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Stamp {
    identity: (u64, u64),
    times: (i64, i64, i64, i64),
    attributes: (u64, u32, u32, u64),
}

impl From<Metadata> for Stamp {
    fn from(m: Metadata) -> Self {
        Self {
            identity: (m.dev(), m.ino()),
            times: (m.mtime(), m.mtime_nsec(), m.ctime(), m.ctime_nsec()),
            attributes: (m.len(), m.uid(), m.mode(), m.nlink()),
        }
    }
}

pub(crate) fn lock(file: &File, shared: bool, control: &Control) -> Result<()> {
    let stop = Instant::now() + Duration::from_secs(2);
    loop {
        control.check()?;
        match if shared {
            file.try_lock_shared()
        } else {
            file.try_lock()
        } {
            Ok(()) => return Ok(()),
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        if Instant::now() >= stop {
            return Err(Error::Busy);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

pub(crate) fn slots() -> impl Iterator<Item = String> {
    (0..8).map(|i| format!("reader-{i}"))
}

pub(crate) fn reader_slot(root: &File, control: &Control) -> Result<File> {
    let stop = Instant::now() + Duration::from_secs(2);
    loop {
        control.check()?;
        for name in slots() {
            let f = control_file(root, &name, false)?;
            match f.try_lock() {
                Ok(()) => return Ok(f),
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Error(e)) => return Err(e.into()),
            }
        }
        if Instant::now() >= stop {
            return Err(Error::Busy);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

pub(crate) fn control_file(root: &File, name: &str, create: bool) -> Result<File> {
    let extra = if create {
        OFlags::CREATE
    } else {
        OFlags::empty()
    };
    let file = data(root, name, OFlags::RDWR | extra, true)?;
    if file.metadata()?.len() != 0 {
        return Err(Error::Integrity("lock file size"));
    }
    Ok(file)
}
