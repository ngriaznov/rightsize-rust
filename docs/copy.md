# Copying Files

Two different ways to get files into a container, for two different moments in a
container's life.

## Runtime copy vs. start-time mount — when to use which

`Container::with_copy_file_to_container` (covered in
[Files & Resources](./core-concepts/files-and-resources.md)) configures a mount
**before boot** — it's part of the spec the container starts from, and it never
changes again for that container's lifetime.

The operations on this page run **after** `start()`, against an already-running
container — useful when the file to copy doesn't exist yet at boot time (a
generated fixture, a config rendered from a template, output the guest itself
produced) or when you only decide what to copy in partway through a test.

```rust,ignore
use rightsize::Container;

let guard = Container::new("alpine:3.19")
    .with_command(&["sleep", "120"])
    .start()
    .await?;

// Any time after start() — not just once, not just before boot:
guard.copy_file_to_container("/host/path/config.json", "/etc/app/config.json").await?;
```

## The three operations

```rust,ignore
// Host file or directory -> guest.
guard.copy_file_to_container(host_path, "/guest/path").await?;

// In-memory bytes or a string -> guest (writes a private temp file under the hood,
// delegates to copy_file_to_container, cleans the temp file up afterward).
guard.copy_content_to_container(b"hello\n", "/guest/path/hello.txt").await?;
guard.copy_content_to_container("hello\n", "/guest/path/hello.txt").await?;

// Guest file or directory -> host.
guard.copy_file_from_container("/guest/path", host_dest_path).await?;
```

There is no separate "directory" method on either side — each one accepts a file
OR a directory source, exactly like `cp -r`/`docker cp`/`msb copy` themselves do.

## Directory semantics

Copying a directory to an absent destination produces the destination as a copy of
the source — contents land directly under the destination, not nested one level
deeper inside it:

```rust,ignore
// /host/fixtures/{a.txt, sub/b.txt} exists; /guest/dst does not.
guard.copy_file_to_container("/host/fixtures", "/guest/dst").await?;
// -> /guest/dst/a.txt, /guest/dst/sub/b.txt
```

The same shape applies copying a directory out. This matches how `cp -r`/
`docker cp`/`msb copy` already behave against a nonexistent destination — nothing
rightsize-rust invents on top of the underlying tools.

## The parent-creation guarantee

You never have to pre-create a destination's parent directory, on either side:

- **Copying in**: the guest's destination parent is created first
  (`exec: mkdir -p <parent>`), before the transfer.
- **Copying out**: the host destination's parent is created via the standard
  library (`std::fs::create_dir_all`), before the transfer.

## Requirements and failure modes

- **The container must be running.** A never-started or stopped guard fails with
  the same "not running" error shape `exec`/`logs` use, before any backend call.
- **The container path must be absolute.** Both `msb copy` and `docker cp` require
  a `NAME:/abs/path` shape; a relative path fails fast with an actionable message,
  before any backend call.
- **A failed copy surfaces the tool's stderr.** A missing source in the guest, a
  permission error, and so on raise the backend-error shape carrying the
  underlying `msb`/`docker` stderr — never a silent success.

## Reuse caveat

Runtime copies work on a [reuse](./reuse.md) container exactly like any other
runtime operation — but they mutate shared reused state, and they are **not** part
of the reuse identity hash. Two reuse containers with an identical spec still adopt
the same sandbox even if one process has copied files into it that the other
never touched; if your workload's correctness depends on a copied-in file's
presence or content, that dependency is on you to manage, not on the identity hash.

## Backend implementation notes

- **microsandbox**: `msb copy -q <src> <name>:<dst>` (and the mirrored form for
  copy-out) — the same invocation plumbing this backend uses for every other `msb`
  subcommand.
- **docker**: shells out to the `docker` CLI (`docker cp`) rather than the daemon's
  own tar-archive-in/tar-archive-out endpoints — hand-rolling tar encode/decode, or
  adding a dependency for it, would be worse than reusing a CLI this backend
  already requires (the [orphan-reaping](./reaping.md) watchdog's own kill command
  is a plain `docker rm -f`).
