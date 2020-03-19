#[macro_use]
extern crate log;

use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};
use libc::{EACCES, EEXIST, ENOENT, ENOSYS, ENOTEMPTY};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

use std::collections::HashMap;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::fs::{metadata, File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{ErrorKind, SeekFrom};
use std::process::Command;
use time::Timespec;

use walkdir::WalkDir;

const target: &'static str = "/tmp/target";

fn get_file_name_from_inode(ino: u64) -> Option<String> {
    if ino == 1 {
        return Some(target.to_string());
    } else {
        for entry in WalkDir::new(target)
            .into_iter()
            .map(|e| e.unwrap())
            .filter(|e| e.path().to_str().unwrap() != target)
        {
            // let entry = entry.unwrap();
            if entry.metadata().unwrap().ino() == ino {
                return Some(entry.path().to_str().unwrap().to_string());
            }
        }
    }
    None
}

fn real_path<T: std::fmt::Display>(pth: T) -> String {
    format!("{}/{}", target, pth)
}

fn get_attr<T: std::convert::AsRef<std::path::Path>>(pth: T) -> FileAttr {
    let attrs = metadata(pth).unwrap();
    // debug!("<>{}", attrs.rdev());
    FileAttr {
        ino: attrs.ino(),
        size: attrs.size(),
        blocks: attrs.blocks(),
        atime: Timespec::new(attrs.atime(), 0),
        mtime: Timespec::new(attrs.mtime(), 0),
        ctime: Timespec::new(attrs.ctime(), 0),
        crtime: Timespec::new(0, 0), // macos only
        kind: {
            let typ = attrs.file_type();
            if typ.is_dir() {
                FileType::Directory
            // } else if typ.is_file() {
            } else {
                FileType::RegularFile
                // reply.error(ENOSYS);
                // return;
            }
        },
        perm: attrs.permissions().mode() as u16,
        nlink: attrs.nlink() as u32,
        uid: attrs.uid(),
        gid: attrs.gid(),
        // rdev: 0,
        rdev: attrs.dev() as u32,
        flags: 0, // macos only
    }
}

struct ThanosFS {
    last_fh: u64,
    open_file_handles: HashMap<u64, File>,
}

impl Filesystem for ThanosFS {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr(ino={})", ino);
        let file_name = get_file_name_from_inode(ino).unwrap();
        debug!("getattr(file_name={})", file_name);
        let fileattr = get_attr(file_name);
        reply.attr(&Timespec::new(1, 0), &fileattr);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir(ino={}, fh={}, offset={})", _ino, _fh, _offset);
        let file_name = get_file_name_from_inode(_ino).unwrap();
        debug!("readdir(file_name={})", file_name);
        for (i, dir) in fs::read_dir(file_name)
            .unwrap()
            .skip(_offset as usize)
            .map(|res| res.unwrap())
            .enumerate()
        {
            reply.add(
                dir.metadata().unwrap().ino(),
                (i + 1) as i64,
                {
                    let typ = dir.metadata().unwrap().file_type();
                    if typ.is_dir() {
                        FileType::Directory
                    } else if typ.is_file() {
                        FileType::RegularFile
                    } else {
                        reply.error(ENOSYS);
                        return;
                    }
                },
                dir.file_name(),
            );
        }
        reply.ok();
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!(
            "lookup(parent_ino={}, name={})",
            parent,
            name.to_str().unwrap()
        );
        let file_name = get_file_name_from_inode(parent).unwrap();
        debug!("lookup(file_name={})", file_name);
        for dir in fs::read_dir(file_name.clone())
            .unwrap()
            .map(|res| res.unwrap())
        {
            if dir.file_name() == name.to_str().unwrap() {
                let real_path = format!("{}/{}", file_name, dir.file_name().to_str().unwrap());
                // let real_path = real_path(dir.file_name().to_str().unwrap());
                debug!("lookup(real_path={})", real_path);

                reply.entry(&Timespec::new(1, 0), &get_attr(real_path), 0);
                return;
            }
        }

        reply.error(ENOENT);
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        reply: ReplyEntry,
    ) {
        debug!(
            "mkdir(parent={} name={} mode={})",
            _parent,
            _name.to_str().unwrap(),
            _mode
        );
        let file_name = get_file_name_from_inode(_parent).unwrap();
        debug!("mkdir(file_name={})", file_name);
        let real_path = format!("{}/{}", file_name, _name.to_str().unwrap());

        fs::create_dir(&real_path).unwrap();
        let metadata = fs::metadata(&real_path).unwrap();
        let mut perms = metadata.permissions();
        perms.set_mode(_mode);
        fs::set_permissions(&real_path, perms).unwrap();

        reply.entry(&Timespec::new(1, 0), &get_attr(real_path), 0);

        // reply.error(ENOSYS);
    }

    fn rmdir(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        debug!("rmdir(parent={} name={})", _parent, _name.to_str().unwrap(),);
        let file_name = get_file_name_from_inode(_parent).unwrap();
        debug!("rmdir(file_name={})", file_name);
        let real_path = format!("{}/{}", file_name, _name.to_str().unwrap());
        debug!("rmdir(real_path={})", real_path);
        // fs::remove_dir(&real_path).unwrap();
        // reply.ok();
        match fs::remove_dir(&real_path) {
            Ok(()) => reply.ok(),
            Err(e) => match e.kind() {
                ErrorKind::PermissionDenied => reply.error(EACCES),
                ErrorKind::Other => reply.error(ENOTEMPTY),
                _ => reply.error(ENOSYS),
            },
        }
    }

    fn open(&mut self, _req: &Request, _ino: u64, _flags: u32, reply: ReplyOpen) {
        debug!("open(ino={} flags={})", _ino, _flags);
        let file_name = get_file_name_from_inode(_ino).unwrap();
        debug!("open(file_name={})", file_name);

        // FIXME work around to make open work with both read/write/create operations
        // since I wan't able to make _flags arguments work.
        //
        // Another option here would be to make open do nothing and open the
        // file appropriately in read/write methods
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name);

        match file {
            Ok(file) => {
                self.last_fh += 1;
                debug!("open_aok(fh={})", self.last_fh);
                self.open_file_handles.insert(self.last_fh, file);
                reply.opened(self.last_fh, _flags);
            }
            Err(e) => {
                debug!("open(error={})", e);
                match e.raw_os_error() {
                    Some(err) => reply.error(err),
                    _ => reply.error(ENOSYS),
                }
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _size: u32,
        reply: ReplyData,
    ) {
        debug!(
            "read(ino={} fh={} offest={} size {})",
            _ino, _fh, _offset, _size
        );
        // TODO below is only for debug purpose, remove when done
        let file_name = get_file_name_from_inode(_ino).unwrap();
        debug!("read(file_name={})", file_name);

        let mut file = self.open_file_handles.get(&_fh).unwrap();
        // TODO handle _fh not in dictionary, sould open a file and do the operation
        file.seek(SeekFrom::Start(_offset as u64));
        let mut buf = vec![0; _size as usize];
        match file.read(&mut buf) {
            Ok(nbytes) => {
                debug!("read_aok(nbytes={})", nbytes);
                reply.data(&buf)
            }
            Err(e) => {
                debug!("read(error={})", e);
                match e.raw_os_error() {
                    Some(err) => reply.error(err),
                    _ => reply.error(ENOSYS),
                }
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        let mut file = self.open_file_handles.get(&_fh).unwrap();
        // TODO handle _fh not in dictionary,
        file.seek(SeekFrom::Start(_offset as u64));

        // FIXME this is buggy
        //let file_name = get_file_name_from_inode(_ino).unwrap();
        //let mut file = OpenOptions::new().write(true).open(file_name).unwrap();

        match file.write(_data) {
            Ok(nbytes) => {
                debug!("write_aok(nbytes={})", nbytes);
                reply.written(nbytes as u32)
            }
            Err(e) => {
                debug!("write(error={})", e);
                match e.raw_os_error() {
                    Some(err) => reply.error(err),
                    _ => reply.error(ENOSYS),
                }
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request,
        _ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<Timespec>,
        _mtime: Option<Timespec>,
        _fh: Option<u64>,
        _crtime: Option<Timespec>,
        _chgtime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        debug!("setattr(ino={})", _ino);
        let file_name = get_file_name_from_inode(_ino).unwrap();
        debug!("setattr(file_name={})", file_name);

        let fileattr = get_attr(file_name);
        reply.attr(&Timespec::new(1, 0), &fileattr);
    }

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        let mut file = self.open_file_handles.get(&_fh).unwrap();
        // TODO handle _fh not in dictionary,
        match file.flush() {
            Ok(_) => {
                debug!("flush(ok)");
                reply.ok();
            }
            Err(_) => {
                debug!("flush(err)");
                reply.error(ENOSYS)
            }
        }
    }
}

fn main() {
    env_logger::init();
    // let mountpoint = env::args_os().nth(1).unwrap();
    let mountpoint = "/tmp/fuse";

    debug!("register callback for sigterm");
    ctrlc::set_handler(|| {
        debug!("attempting unmount");
        Command::new("fusermount")
            .args(&["-u", "/tmp/fuse"])
            .output()
            .expect("error running unmount command");
        debug!("unmount successful");
    })
    .expect("Error setting Ctrl-C handler");

    let fs = ThanosFS {
        last_fh: 0, // monotonically increasing counter used for unique fh numbers, ??would random uuid would be better here??
        open_file_handles: HashMap::new(),
    };

    fuse::mount(fs, &mountpoint, &[]).unwrap();
}
