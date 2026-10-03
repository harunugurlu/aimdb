# aimdb-sync

Synchronous access to AimDB for FFI bindings, legacy applications, scripts,
and other code that does not run an async executor.

## Overview

`aimdb-sync` starts a dedicated background thread that owns a Tokio runtime and
drives AimDB's async tasks. Callers use ordinary synchronous methods:

- `SyncProducer::set()` checks the runtime and record, then pushes directly
  into the record buffer.
- `SyncConsumer::try_get()` reads immediately from its `Reader<T>`.
- Blocking consumer methods use Tokio's `block_on` only when a read must wait.
- `SyncProducer` is `Clone + Send + Sync`; `SyncConsumer` is `Send`, but is not
  `Clone` or `Sync`, because it owns a mutable subscription cursor.

## Architecture

```text
Synchronous caller
  │
  ├─ SyncProducer::set()
  │     └─ runtime/fork check → keyed record lookup → direct buffer push
  │
  └─ SyncConsumer
        └─ pre-resolved Reader<T> → try immediately or block while waiting

AimDB record buffers are shared with the dedicated background thread, which
owns the Tokio runtime and drives AimDB's async sources, taps and connectors.
```

Creating a producer is lazy: it stores the record key and a weak runtime
reference. Key and type errors therefore surface on `set()`, where the key is
first used. Creating a consumer resolves its reader immediately and can fail if
the key, type or buffer configuration is invalid.

## Quick Start

Add the crates to `Cargo.toml`:

```toml
[dependencies]
aimdb-sync = "0.6"
aimdb-core = "2.0"
aimdb-tokio-adapter = "0.7"
```

```rust
use aimdb_core::{buffer::BufferCfg, AimDbBuilder};
use aimdb_sync::AimDbBuilderSyncExt;
use aimdb_tokio_adapter::{TokioAdapter, TokioRecordRegistrarExt};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
struct Temperature {
    celsius: f32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = AimDbBuilder::new().runtime(Arc::new(TokioAdapter));
    builder.configure::<Temperature>("sensor.temperature", |reg| {
        reg.buffer(BufferCfg::SpmcRing { capacity: 16 });
    });

    let handle = builder.attach()?;
    let producer = handle.producer::<Temperature>("sensor.temperature")?;
    let mut consumer = handle.consumer::<Temperature>("sensor.temperature")?;

    producer.set(Temperature { celsius: 23.5 })?;

    let value = consumer.get_with_timeout(Duration::from_secs(1))?;
    println!("Temperature: {}°C", value.celsius);

    handle.detach()?;
    Ok(())
}
```

Do not call `attach()` or a blocking consumer method from a thread that is
already driving a Tokio runtime. Tokio's blocking entry points panic in that
context.

## Producer Operations

`set()` is a synchronous direct write. It does not wait for free buffer space:
all built-in buffers accept the value and apply their overwrite semantics.

```rust
let producer = handle.producer::<Temperature>("sensor.temperature")?;
producer.set(Temperature { celsius: 24.0 })?;
```

A producer is cheap to clone and may be shared across threads:

```rust
let worker_producer = producer.clone();
std::thread::spawn(move || {
    worker_producer
        .set(Temperature { celsius: 25.0 })
        .expect("runtime and record should remain available");
});
```

## Consumer Operations

Consumer methods take `&mut self` because every read advances that consumer's
cursor.

```rust
use std::time::Duration;

let mut consumer = handle.consumer::<Temperature>("sensor.temperature")?;

// Wait until a value arrives.
let next = consumer.get()?;

// Wait up to the supplied duration.
let next_with_deadline = consumer.get_with_timeout(Duration::from_secs(1))?;

// Return immediately; GetTimeout means no value is currently available.
let maybe_next = consumer.try_get();

// Wait for one value, then drain queued values and return the newest one.
let latest = consumer.get_latest()?;

# let _ = (next, next_with_deadline, maybe_next, latest);
# Ok::<(), aimdb_sync::SyncError>(())
```

`get_latest_with_timeout()` applies its timeout only while waiting for the first
value. Draining values already arriving afterward is not time-bounded.

## Multiple Consumers

Call `handle.consumer()` separately for every independent subscription. Each
consumer has its own cursor and sees every value available to that subscription.

```rust
let consumer_one = handle.consumer::<Temperature>("sensor.temperature")?;
let consumer_two = handle.consumer::<Temperature>("sensor.temperature")?;

let first_thread = std::thread::spawn(move || {
    let mut consumer = consumer_one;
    consumer.get()
});

let second_thread = std::thread::spawn(move || {
    let mut consumer = consumer_two;
    consumer.get()
});

# let _ = (first_thread, second_thread);
# Ok::<(), aimdb_sync::SyncError>(())
```

To split one stream among workers instead, share one consumer behind
`Arc<Mutex<SyncConsumer<T>>>`. That serializes access to its single cursor.

## Buffer Types

```rust
use aimdb_core::buffer::BufferCfg;

// Bounded history for independent consumers. New writes overwrite the oldest
// values when the capacity is exceeded; a slow reader observes BufferLagged.
builder.configure::<MyData>("history", |reg| {
    reg.buffer(BufferCfg::SpmcRing { capacity: 100 });
});

// Retains the newest state. Each write replaces the previous value.
builder.configure::<MyData>("latest", |reg| {
    reg.buffer(BufferCfg::SingleLatest);
});

// One pending value. A write overwrites an unconsumed value; a successful read
// takes and clears the slot.
builder.configure::<MyData>("mailbox", |reg| {
    reg.buffer(BufferCfg::Mailbox);
});
```

## Errors

All facade operations use `SyncResult<T>`, whose error is `SyncError`:

- `RuntimeShutdown`: the handle has shut down and a producer can no longer
  reach the database.
- `ForkedChild`: the object was inherited by a Unix child process without its
  background runtime thread.
- `GetTimeout`: a timed read expired, or `try_get()` found no value.
- `AttachFailed` / `DetachFailed`: starting or stopping the runtime failed.
- `Db(DbError)`: the underlying database rejected the operation, for example
  because a key was missing, named another type, or a reader lagged.

`SyncError` is non-exhaustive. Match the cases you need and retain a fallback:

```rust
use aimdb_core::DbError;
use aimdb_sync::SyncError;

match producer.set(Temperature { celsius: 26.0 }) {
    Ok(()) => println!("written"),
    Err(SyncError::RuntimeShutdown) => eprintln!("runtime is shut down"),
    Err(SyncError::ForkedChild) => eprintln!("runtime thread did not survive fork"),
    Err(SyncError::Db(DbError::RecordKeyNotFound { .. })) => {
        eprintln!("record key is not registered")
    }
    Err(error) => eprintln!("write failed: {error}"),
}
```

For an SPMC ring, `get()`, `get_with_timeout()` and `try_get()` expose lag as
`SyncError::Db(DbError::BufferLagged { .. })`. The `get_latest` methods skip lag
while catching up to the newest available value.

## Shutdown

Prefer explicit shutdown so the runtime thread is stopped and joined before
the program continues:

```rust
// Shared-reference form: idempotent and suitable for an FFI-owned handle.
handle.shutdown()?;

// Or, when ownership can be consumed:
// handle.detach()?;

// Timed forms are also available:
// handle.shutdown_timeout(Duration::from_secs(5))?;
// handle.detach_timeout(Duration::from_secs(5))?;
```

Shutdown releases the database and closes its buffers, waking consumers that
are waiting for data. Dropping a live handle signals shutdown but does not wait
for the thread; it also logs a warning, so explicit shutdown is recommended.

## Practical Guidance

- Use `try_get()` when the caller must not wait.
- Give each thread its own consumer when every thread should see every value.
- Use explicit shutdown, especially at an FFI boundary.
- Treat `BufferLagged` as a recoverable signal that an SPMC reader fell behind.

## Testing

```bash
cargo test -p aimdb-sync
cargo test -p aimdb-sync --all-features
```

The repository also contains `examples/sync-api-demo` for a complete synchronous
integration.

## Documentation

```bash
cargo doc -p aimdb-sync --open
```

## License

See [LICENSE](../LICENSE).
