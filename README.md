# thanos-fs
thanos-fs is a completely balanced FUSE filesystem, as all filesystems should be.
On every write to a file, half of the bytes will be thrown away, which half is chosen randomly.


## Usage

| :exclamation: :skull:  YOU WILL LOSE DATA :skull: :exclamation: Do not use a directory with anything useful as mount target.   |
|-----------------------------------------|
|Do not run this if you are unsure what it does.|


```
cargo run -- --taget-dir /target/dir --mount-dir /mount/dir
```

All the operations to mount dir will be passes through to target dir with the only exception
that each write ignores half of the data.


The application will try to unmount the filesystem in case it receives a SIGTERM or SIGINT.
If that fails for some reason you can try to unmount manually

```
fusermount -u /path/to/mount/point
```

### Operations not supported

- Streaming directory operations
- Extended file attributes
- fsync and fsyncdir
- POSIX file locks
