use std::ffi::CStr;

use log::info;
use radixdb::BlobStore;
use sqlite_vfs::{OpenOptions, VfsError, VfsResult};

use crate::{SyncDir, SyncFile};

#[derive(Debug)]
pub struct DatabaseFile {
    inner: SyncFile,
}

impl DatabaseFile {
    fn new(file: SyncFile) -> Self {
        Self { inner: file }
    }
}

impl sqlite_vfs::File for DatabaseFile {
    fn read(&mut self, start: u64, buf: &mut [u8]) -> VfsResult<usize> {
        info!("read_exact {:?}, {}", self, buf.len());
        let current_len = self.inner.length().map_err(anyhow_to_vfs)?;
        let end = current_len.min(start + buf.len() as u64);
        let range = start..end;
        let len = end.saturating_sub(start);
        let data = self.inner.read(range).map_err(anyhow_to_vfs);
        let data = data?;
        let len_usize = len as usize;
        buf[0..len_usize].copy_from_slice(&data[..len_usize]);
        Ok(len_usize)
    }

    fn write(&mut self, start: u64, buf: &[u8]) -> VfsResult<usize> {
        info!("write_all {:?} {} {}", self, start, buf.len());
        let res = self
            .inner
            .write(start, buf.to_vec())
            .map_err(anyhow_to_vfs)?;
        Ok(buf.len())
    }

    fn file_size(&self) -> VfsResult<u64> {
        info!("file_size {:?}", self);
        self.inner.length().map_err(anyhow_to_vfs)
    }

    fn truncate(&mut self, size: u64) -> VfsResult<()> {
        info!("truncate {:?} {}", self, size);
        self.inner.truncate(size).map_err(anyhow_to_vfs)
    }

    fn sync(&mut self) -> VfsResult<()> {
        self.inner.flush().map_err(anyhow_to_vfs)
    }
}

fn anyhow_to_vfs(_: anyhow::Error) -> VfsError {
    sqlite_vfs::SQLITE_IOERR
}

impl sqlite_vfs::Vfs for SyncDir {
    type File = Box<dyn sqlite_vfs::File>;

    fn open(&mut self, path: &CStr, opts: OpenOptions) -> VfsResult<Self::File> {
        let path = path.to_string_lossy();
        info!("open {:?} {} {:?}", self, path, opts);
        let inner = self.open_file(path).map_err(anyhow_to_vfs)?;
        Ok(Box::new(DatabaseFile::new(inner)))
    }

    fn delete(&mut self, path: &CStr) -> VfsResult<()> {
        let path = path.to_string_lossy();
        info!("delete {:?} {}", self, path);
        self.delete_file(path).map_err(anyhow_to_vfs)?;
        Ok(())
    }

    fn exists(&mut self, path: &CStr) -> VfsResult<bool> {
        let path = path.to_string_lossy();
        info!("exists {:?} {}", self, path);
        let res = self.file_exists(path).map_err(anyhow_to_vfs)?;
        Ok(res)
    }
}
