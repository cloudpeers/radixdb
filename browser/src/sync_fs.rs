use futures::channel::mpsc::UnboundedSender;
use futures::{
    channel::{mpsc, oneshot},
    executor::block_on,
};
use radixdb::Blob;
use std::fmt::Debug;
use std::ops::Range;

use crate::SharedStr;

/// protocol for syncfs
#[derive(Debug)]
pub(crate) enum Command {
    OpenDir {
        dir_name: SharedStr,
        cb: oneshot::Sender<anyhow::Result<()>>,
    },
    DeleteDir {
        dir_name: SharedStr,
        cb: oneshot::Sender<anyhow::Result<bool>>,
    },
    OpenFile {
        dir_name: SharedStr,
        file_name: SharedStr,
        cb: oneshot::Sender<anyhow::Result<()>>,
    },
    FileExists {
        dir_name: SharedStr,
        file_name: SharedStr,
        cb: oneshot::Sender<anyhow::Result<bool>>,
    },
    DeleteFile {
        dir_name: SharedStr,
        file_name: SharedStr,
        cb: oneshot::Sender<anyhow::Result<bool>>,
    },
    FlushFile {
        dir_name: SharedStr,
        file_name: SharedStr,
        cb: oneshot::Sender<anyhow::Result<()>>,
    },
    Write {
        dir_name: SharedStr,
        file_name: SharedStr,
        offset: u64,
        data: Vec<u8>,
        cb: oneshot::Sender<anyhow::Result<()>>,
    },
    Read {
        dir_name: SharedStr,
        file_name: SharedStr,
        range: Range<u64>,
        cb: oneshot::Sender<anyhow::Result<Vec<u8>>>,
    },
    Length {
        dir_name: SharedStr,
        file_name: SharedStr,
        cb: oneshot::Sender<anyhow::Result<u64>>,
    },
    Truncate {
        dir_name: SharedStr,
        file_name: SharedStr,
        size: u64,
        cb: oneshot::Sender<anyhow::Result<()>>,
    },
    AppendFileLengthPrefixed {
        dir_name: SharedStr,
        file_name: SharedStr,
        data: Vec<u8>,
        cb: oneshot::Sender<anyhow::Result<u64>>,
    },
    ReadFileLengthPrefixed {
        dir_name: SharedStr,
        file_name: SharedStr,
        offset: u64,
        cb: oneshot::Sender<anyhow::Result<Blob<u8>>>,
    },
    Shutdown,
}

/// A synchronous virtual file system that can be backed by different impls depending on the
/// browser capabilities
#[derive(Debug)]
pub struct SyncFs {
    tx: mpsc::UnboundedSender<Command>,
}

impl SyncFs {
    pub(crate) fn new(tx: UnboundedSender<Command>) -> Self {
        Self { tx }
    }

    /// open a directory
    pub fn open_dir(&self, dir_name: impl Into<SharedStr>) -> anyhow::Result<SyncDir> {
        let dir_name = dir_name.into();
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::OpenDir {
            dir_name: dir_name.clone(),
            cb: tx,
        })?;
        block_on(rx)??;
        Ok(SyncDir {
            dir_name,
            tx: self.tx.clone(),
        })
    }

    /// delete a directory
    ///
    /// will return true if there was a directory of that name, otherwise false
    pub fn delete_dir(&self, dir_name: impl Into<SharedStr>) -> anyhow::Result<bool> {
        let dir_name = dir_name.into();
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::DeleteDir {
            dir_name: dir_name.clone(),
            cb: tx,
        })?;
        let res = block_on(rx)??;
        Ok(res)
    }

    /// manual shutdown of all files and directories of this fs
    pub fn unmount(&self) {
        let _ = self.tx.unbounded_send(Command::Shutdown);
    }
}

#[derive(Debug)]
pub struct SyncDir {
    dir_name: SharedStr,
    tx: mpsc::UnboundedSender<Command>,
}

impl SyncDir {
    pub fn open_file(&self, file_name: impl Into<SharedStr>) -> anyhow::Result<SyncFile> {
        let file_name = file_name.into();
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::OpenFile {
            dir_name: self.dir_name.clone(),
            file_name: file_name.clone(),
            cb: tx,
        })?;
        block_on(rx)??;
        Ok(SyncFile {
            dir_name: self.dir_name.clone(),
            file_name: file_name.clone(),
            tx: self.tx.clone(),
        })
    }

    pub fn file_exists(&self, file_name: impl Into<SharedStr>) -> anyhow::Result<bool> {
        let file_name = file_name.into();
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::FileExists {
            dir_name: self.dir_name.clone(),
            file_name: file_name.clone(),
            cb: tx,
        })?;
        let res = block_on(rx)??;
        Ok(res)
    }

    pub fn delete_file(&self, file_name: impl Into<SharedStr>) -> anyhow::Result<bool> {
        let file_name = file_name.into();
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::DeleteFile {
            dir_name: self.dir_name.clone(),
            file_name: file_name.clone(),
            cb: tx,
        })?;
        let res = block_on(rx)??;
        Ok(res)
    }
}

#[derive(Debug)]
pub struct SyncFile {
    dir_name: SharedStr,
    file_name: SharedStr,
    tx: mpsc::UnboundedSender<Command>,
}

impl SyncFile {
    pub fn length(&self) -> anyhow::Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::Length {
            dir_name: self.dir_name.clone(),
            file_name: self.file_name.clone(),
            cb: tx,
        })?;
        block_on(rx)?
    }

    pub fn read(&self, range: Range<u64>) -> anyhow::Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::Read {
            dir_name: self.dir_name.clone(),
            file_name: self.file_name.clone(),
            range,
            cb: tx,
        })?;
        block_on(rx)?
    }

    pub fn write(&self, offset: u64, data: Vec<u8>) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::Write {
            dir_name: self.dir_name.clone(),
            file_name: self.file_name.clone(),
            offset,
            data,
            cb: tx,
        })?;
        block_on(rx)?
    }

    pub fn truncate(&self, size: u64) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::Truncate {
            dir_name: self.dir_name.clone(),
            file_name: self.file_name.clone(),
            size,
            cb: tx,
        })?;
        block_on(rx)?
    }
}

impl radixdb::BlobStore for SyncFile {

    type Error = anyhow::Error;

    fn read(&self, id: u64) -> anyhow::Result<Blob<u8>> {
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::ReadFileLengthPrefixed {
            offset: id,
            dir_name: self.dir_name.clone(),
            file_name: self.file_name.clone(),
            cb: tx,
        })?;
        let data = block_on(rx)??;
        Ok(data)
    }

    fn sync(&self) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::FlushFile {
            dir_name: self.dir_name.clone(),
            file_name: self.file_name.clone(),
            cb: tx,
        })?;
        block_on(rx)??;
        Ok(())
    }

    fn write(&self, data: &[u8]) -> anyhow::Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Command::AppendFileLengthPrefixed {
            dir_name: self.dir_name.clone(),
            file_name: self.file_name.clone(),
            data: data.to_vec(),
            cb: tx,
        })?;
        let offset = block_on(rx)??;
        Ok(offset)
    }
}
