#[macro_use]
extern crate log;

use fuse::{FileAttr, FileType, Filesystem, ReplyAttr, ReplyDirectory, ReplyEntry, Request};
use libc::ENOSYS;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;

use std::env;
use std::ffi::OsStr;
use std::fs;
use std::fs::metadata;
use std::process::Command;
use time::Timespec;

use walkdir::WalkDir;

struct ThanosFS;

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

    fuse::mount(ThanosFS, &mountpoint, &[]).unwrap();
}
