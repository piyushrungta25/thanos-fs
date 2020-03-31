#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::{hard_link, rename, symlink_metadata, File, OpenOptions};
use std::io::prelude::*;
use std::io::{ErrorKind, SeekFrom};
use std::mem;
use std::os::unix::fs::symlink;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyStatfs, ReplyWrite, Request,
};

use nix::errno::Errno;
use nix::fcntl::readlink;
use nix::sys::stat::{mknod, Mode, SFlag};
use nix::sys::statvfs::statvfs;
use nix::unistd::{chown, Gid, Uid};

use clap::{App, Arg};
use rand::random;
use time::Timespec;
use walkdir::WalkDir;

fn get_attr<T: std::convert::AsRef<std::path::Path>>(pth: T) -> FileAttr {
    let attrs = symlink_metadata(pth).unwrap();

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
            } else if typ.is_symlink() {
                FileType::Symlink
            // TODO handle other file types
            } else {
                FileType::RegularFile
            }
        },
        perm: attrs.permissions().mode() as u16,
        nlink: attrs.nlink() as u32,
        uid: attrs.uid(),
        gid: attrs.gid(),
        rdev: attrs.dev() as u32,
        flags: 0, // macos only
    }
}

struct ThanosFS {
    last_fh: u64,
    open_file_handles: HashMap<u64, File>,
    target_dir: String,
}

impl ThanosFS {
    fn get_file_name_from_inode(&mut self, ino: u64) -> Option<String> {
        if ino == 1 {
            return Some(self.target_dir.to_string());
        } else {
            for entry in WalkDir::new(self.target_dir.clone())
                .into_iter()
                .map(|e| e.unwrap())
                .filter(|e| e.path().to_str().unwrap() != self.target_dir)
            {
                // let entry = entry.unwrap();
                if entry.metadata().unwrap().ino() == ino {
                    return Some(entry.path().to_str().unwrap().to_string());
                }
            }
        }
        None
    }
}

impl Filesystem for ThanosFS {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let file_name = self.get_file_name_from_inode(ino).unwrap();
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
        let file_name = self.get_file_name_from_inode(_ino).unwrap();
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
                    } else if typ.is_symlink() {
                        FileType::Symlink
                    // TODO handle other file types
                    } else {
                        reply.error(Errno::ENOSYS as i32);
                        return;
                    }
                },
                dir.file_name(),
            );
        }
        reply.ok();
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_name = self.get_file_name_from_inode(parent).unwrap();
        let file_name = Path::new(&parent_name).join(name);
        if file_name.exists() {
            let fileattr = &get_attr(file_name);
            reply.entry(&Timespec::new(1, 0), fileattr, 0);
        } else {
            reply.error(Errno::ENOENT as i32);
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        reply: ReplyEntry,
    ) {
        let parent_path = self.get_file_name_from_inode(_parent).unwrap();
        debug!("mkdir(file_name={})", parent_path);
        let real_path = Path::new(&parent_path).join(_name);

        fs::create_dir(&real_path).unwrap();
        let metadata = symlink_metadata(&real_path).unwrap();
        let mut perms = metadata.permissions();
        perms.set_mode(_mode);
        fs::set_permissions(&real_path, perms).unwrap();

        reply.entry(&Timespec::new(1, 0), &get_attr(real_path), 0);
    }

    fn rmdir(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        let file_name = self.get_file_name_from_inode(_parent).unwrap();
        let real_path = Path::new(&file_name).join(_name);
        debug!("rmdir(real_path={:?})", real_path);

        match fs::remove_dir(&real_path) {
            Ok(()) => reply.ok(),
            Err(e) => match e.kind() {
                ErrorKind::PermissionDenied => reply.error(Errno::EACCES as i32),
                ErrorKind::Other => reply.error(Errno::ENOTEMPTY as i32),
                _ => reply.error(Errno::ENOSYS as i32),
            },
        }
    }

    fn open(&mut self, _req: &Request, _ino: u64, _flags: u32, reply: ReplyOpen) {
        let file_name = self.get_file_name_from_inode(_ino).unwrap();
        debug!("open(file_name={})", file_name);

        // TODO check and reuse if an open file handle for current file alredy exist in the hashmap

        // FIXME work around to make open work with both read/write/create operations since I wasn't
        // able to make _flags arguments work.
        //
        // Another option here would be to make open do nothing and open the file appropriately in
        // read/write methods(implement stateless file operations)
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name);

        match file {
            Ok(file) => {
                self.last_fh += 1;
                self.open_file_handles.insert(self.last_fh, file);
                reply.opened(self.last_fh, _flags);
            }
            Err(e) => match e.raw_os_error() {
                Some(err) => reply.error(err),
                _ => reply.error(Errno::ENOSYS as i32),
            },
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
        let mut file = self.open_file_handles.get(&_fh).unwrap();
        // TODO handle _fh not in dictionary, sould open a file and do the operation
        file.seek(SeekFrom::Start(_offset as u64)).unwrap();
        let mut buf = vec![0; _size as usize];
        match file.read(&mut buf) {
            Ok(_) => reply.data(&buf),
            Err(e) => match e.raw_os_error() {
                Some(err) => reply.error(err),
                _ => reply.error(Errno::ENOSYS as i32),
            },
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
        let data_len = _data.len() as u32;
        let data = if random() {
            &_data[0..((data_len / 2) as usize)]
        } else {
            &_data[((data_len / 2) as usize)..]
        };
        let mut file = self.open_file_handles.get(&_fh).unwrap();
        // TODO handle _fh not in dictionary,
        file.seek(SeekFrom::Start(_offset as u64)).unwrap();

        match file.write(data) {
            Ok(_) => reply.written(data_len),
            Err(e) => match e.raw_os_error() {
                Some(err) => reply.error(err),
                _ => reply.error(Errno::ENOSYS as i32),
            },
        }
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        // this should make sure that the file handle is closed
        self.open_file_handles.remove(&_fh);
        reply.ok();
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
        let file_name = self.get_file_name_from_inode(_ino).unwrap();
        debug!("setattr(file_name={})", file_name);

        // set size
        let f = OpenOptions::new()
            .write(true)
            .open(file_name.clone())
            .unwrap();

        if let Some(new_len) = _size {
            f.set_len(new_len).unwrap();
        }

        // mode
        if let Some(mode) = _mode {
            let new_perm = PermissionsExt::from_mode(mode);
            f.set_permissions(new_perm).unwrap();
        }

        // uid
        if let Some(uid) = _uid {
            chown(file_name.as_str(), Some(Uid::from_raw(uid)), None).unwrap();
        }

        // gid
        if let Some(gid) = _gid {
            chown(file_name.as_str(), None, Some(Gid::from_raw(gid))).unwrap();
        }

        // TODO implement setattr for time, lookup utimensat

        mem::drop(f); // for good measure
        let fileattr = get_attr(file_name);
        reply.attr(&Timespec::new(1, 0), &fileattr);
    }

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        let mut file = self.open_file_handles.get(&_fh).unwrap();
        // TODO handle _fh not in dictionary,
        match file.flush() {
            Ok(_) => {
                reply.ok();
            }
            Err(_) => reply.error(Errno::ENOSYS as i32),
        }
    }

    fn unlink(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        debug!(
            "unlink(parent_ino={}, name={})",
            _parent,
            _name.to_str().unwrap()
        );
        let parent_name = self.get_file_name_from_inode(_parent).unwrap();
        debug!("lookup(file_name={})", parent_name);
        let parent_path = Path::new(&parent_name);
        let file_path = parent_path.join(_name);
        match fs::remove_file(file_path) {
            Ok(_) => reply.ok(),
            _ => reply.error(Errno::ENOENT as i32),
        }
    }

    fn mknod(
        &mut self,
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let parent_name = self.get_file_name_from_inode(_parent).unwrap();
        let file_name = Path::new(&parent_name).join(_name);
        let res = mknod(
            &file_name,
            SFlag::empty(),
            Mode::from_bits_truncate(_mode),
            _rdev as u64,
        );
        if res.is_ok() {
            reply.entry(&Timespec::new(1, 0), &get_attr(file_name), 0);
        } else {
            reply.error(1);
        }
    }

    fn rename(
        &mut self,
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        let src_parent_name = self.get_file_name_from_inode(_parent).unwrap();
        let src_file_name = Path::new(&src_parent_name).join(_name);

        let target_parent_name = self.get_file_name_from_inode(_newparent).unwrap();
        let target_file_name = Path::new(&target_parent_name).join(_newname);

        debug!(
            "rename(src={:?}, trgt={:?})",
            src_file_name, target_file_name
        );

        let res = rename(src_file_name, target_file_name);

        if res.is_ok() {
            reply.ok();
        } else {
            reply.error(1);
        }
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        let file_name = self.get_file_name_from_inode(_ino).unwrap();
        let res = statvfs(file_name.as_str());

        if let Ok(stat) = res {
            reply.statfs(
                stat.blocks(),
                stat.blocks_free(),
                stat.blocks_available(),
                stat.files(),
                stat.files_free(),
                stat.block_size() as u32,
                stat.name_max() as u32,
                stat.fragment_size() as u32,
            );
        } else {
            reply.error(1);
        }
    }

    fn readlink(&mut self, _req: &Request, _ino: u64, reply: ReplyData) {
        let file_name = self.get_file_name_from_inode(_ino).unwrap();
        let res = readlink(file_name.as_str());
        if let Ok(data) = res {
            reply.data(data.to_str().unwrap().as_bytes());
        } else {
            reply.error(1);
        }
    }

    fn symlink(
        &mut self,
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _link: &Path,
        reply: ReplyEntry,
    ) {
        let parent_name = self.get_file_name_from_inode(_parent).unwrap();
        let tgt = Path::new(&parent_name).join(_name);
        let res = symlink(_link, &tgt);
        if res.is_ok() {
            reply.entry(&Timespec::new(1, 0), &get_attr(tgt), 0);
        } else {
            reply.error(1);
        }
    }
    fn link(
        &mut self,
        _req: &Request,
        _ino: u64,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let src = self.get_file_name_from_inode(_ino).unwrap();
        let target_parent = self.get_file_name_from_inode(_newparent).unwrap();
        let tgt = Path::new(&target_parent).join(_newname);
        let res = hard_link(src, &tgt);
        if res.is_ok() {
            reply.entry(&Timespec::new(1, 0), &get_attr(tgt), 0);
        } else {
            reply.error(1);
        }
    }
}
fn main() {
    env_logger::init();

    let matches = App::new("thanos-fs")
        .version("0.1")
        .author("Piyush Rungta <piyushrungta25@gmail.com>")
        .about("A completely balanced FUSE filesystem")
        .arg(
            Arg::with_name("target_dir")
                .long("target-dir")
                .value_name("TARGET_DIR")
                .help("Target dir to passthorugh all the operations")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("mount_point")
                .long("mount-dir")
                .value_name("MOUNT_DIR")
                .help("Directory to mount the filesystem to")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let mount_point = matches.value_of("mount_point").unwrap().to_string();
    debug!("register callback for sigterm");
    ctrlc::set_handler(move || {
        debug!("attempting unmount");
        Command::new("fusermount")
            .args(&["-u", mount_point.as_str()])
            .output()
            .expect("error running unmount command");
        debug!("unmount successful");
    })
    .expect("Error setting Ctrl-C handler");

    let fs = ThanosFS {
        last_fh: 0, // monotonically increasing counter used for unique fh numbers, ??would random uuid would be better here??
        open_file_handles: HashMap::new(),
        target_dir: matches.value_of("target_dir").unwrap().to_string(),
    };

    fuse::mount(
        fs,
        &PathBuf::from(matches.value_of("mount_point").unwrap()),
        &[OsStr::new("allow_other"), OsStr::new("allow_root")],
    )
    .unwrap();
}
