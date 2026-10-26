//! Owner-private staging and publication shared by offline restore and OAuth.
use memento_core::{Error, Result};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::{
        ffi::OsStrExt,
        fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Component, Path, PathBuf},
};
pub fn plain(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut current = PathBuf::new();
    for part in absolute.components() {
        if matches!(part, Component::ParentDir) {
            return Err(Error::Validation("Parent traversal is not allowed"));
        }
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(info) if info.file_type().is_symlink() => {
                return Err(Error::Validation("Symbolic links are not allowed"));
            }
            Ok(_) => (),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(absolute)
}
pub fn private_dir(path: &Path) -> Result<PathBuf> {
    let path = plain(path)?;
    let info = fs::symlink_metadata(&path)?;
    if !info.is_dir() || info.uid() != unsafe { libc::getuid() } || info.mode() & 0o077 != 0 {
        return Err(Error::Validation("An owner-private directory is required"));
    }
    Ok(path)
}
pub fn regular(path: &Path) -> Result<File> {
    plain(path)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(Error::Validation("A regular file is required"));
    }
    Ok(file)
}
pub fn read_private(path: &Path, max: u64) -> Result<Vec<u8>> {
    let file = regular(path)?;
    let info = file.metadata()?;
    if info.mode() & 0o077 != 0
        || ![0, unsafe { libc::getuid() }].contains(&info.uid())
        || info.len() > max
    {
        return Err(Error::Validation(
            "An owner-private regular configuration is required",
        ));
    }
    let mut bytes = Vec::new();
    file.take(max + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max {
        return Err(Error::Validation("Configuration exceeds size limit"));
    }
    Ok(bytes)
}
pub fn create(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .write(true)
        .read(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?)
}
pub fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}
pub fn atomic_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = private_dir(
        path.parent()
            .ok_or(Error::Validation("Configuration needs a parent directory"))?,
    )?;
    plain(path)?;
    let temporary = parent.join(format!(
        ".memento-publish-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let cleanup = RemoveFile(temporary.clone());
    let mut file = create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    sync_dir(&parent)?;
    drop(cleanup);
    Ok(())
}
pub struct RemoveFile(pub PathBuf);
impl Drop for RemoveFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
pub struct Stage(pub PathBuf);
impl Stage {
    pub fn new(parent: &Path, prefix: &str) -> Result<Self> {
        private_dir(parent)?;
        let path = parent.join(format!(".{prefix}-{}", uuid::Uuid::new_v4().simple()));
        fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
}
impl Drop for Stage {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
#[derive(Clone, Copy)]
pub struct Destination {
    identity: Option<(u64, u64)>,
}
impl Destination {
    pub fn inspect(path: &Path) -> Result<Self> {
        plain(path)?;
        let identity = match fs::symlink_metadata(path) {
            Ok(info) => {
                private_dir(path)?;
                if fs::read_dir(path)?.next().is_some() {
                    return Err(Error::Conflict("Restore destination must be empty"));
                }
                Some((info.dev(), info.ino()))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        Ok(Self { identity })
    }
    pub fn publish(self, stage: &Path, path: &Path) -> Result<()> {
        let current = Self::inspect(path)?;
        if self.identity != current.identity {
            return Err(Error::Conflict(
                "Restore destination changed during validation",
            ));
        }
        if self.identity.is_none() {
            let old = std::ffi::CString::new(stage.as_os_str().as_bytes())
                .map_err(|_| Error::Validation("Invalid stage path"))?;
            let new = std::ffi::CString::new(path.as_os_str().as_bytes())
                .map_err(|_| Error::Validation("Invalid destination path"))?;
            let result = unsafe {
                libc::renameat2(
                    libc::AT_FDCWD,
                    old.as_ptr(),
                    libc::AT_FDCWD,
                    new.as_ptr(),
                    libc::RENAME_NOREPLACE,
                )
            };
            if result != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        } else {
            fs::rename(stage, path)?;
        }
        sync_dir(path.parent().unwrap())
    }
}
pub fn sync_tree(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let info = entry.file_type()?;
        if info.is_symlink() {
            return Err(Error::Validation("Staged restore contains a link"));
        }
        if info.is_dir() {
            sync_tree(&entry.path())?;
        } else if info.is_file() {
            File::open(entry.path())?.sync_all()?;
        } else {
            return Err(Error::Validation(
                "Staged restore contains an unsupported file",
            ));
        }
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    sync_dir(path)
}
