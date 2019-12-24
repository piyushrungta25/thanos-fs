#[macro_use]
extern crate log;

use fuse::Filesystem;
use std::env;
use std::process::Command;

struct NullFS;

impl Filesystem for NullFS {}

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

    fuse::mount(NullFS, &mountpoint, &[]).unwrap();
}
