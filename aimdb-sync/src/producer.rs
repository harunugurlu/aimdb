//! Synchronous producer for typed records.

use crate::{SyncError, SyncResult};
use aimdb_core::{AimDb, Producer as CoreProducer};
use alloc::sync::Weak;
use core::fmt::Debug;

/// Synchronous producer for records of type `T`.
///
/// Thread-safe, can be cloned and shared across threads.
/// Values are moved (not cloned) directly into the record's buffer.
///
/// # Thread Safety
///
/// Multiple clones of `SyncProducer<T>` can be used concurrently from
/// different threads. Each `set()` operation is independent and thread-safe.
///
/// # Example
///
/// ```rust,no_run
/// # use aimdb_sync::*;
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Temperature { celsius: f32 }
/// # fn example(producer: &SyncProducer<Temperature>) -> SyncResult<()> {
/// // Set value
/// producer.set(Temperature { celsius: 25.0 })?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SyncProducer<T>
where
    T: Send + 'static + Debug + Clone,
{
    db: Weak<AimDb>,
    producer: CoreProducer<T>,
}

impl<T> SyncProducer<T>
where
    T: Send + 'static + Debug + Clone,
{
    /// Create a new sync producer (internal use only)
    pub(crate) fn new(db: Weak<AimDb>, producer: CoreProducer<T>) -> Self {
        Self { db, producer }
    }

    /// Set the value directly to the buffer.
    ///
    /// Fails if the lookup of the given key fails.
    ///
    /// # Errors
    ///
    /// Returns `SyncError::RuntimeShutdown` if the runtime thread has been detached.
    /// Returns any error from the underlying `produce()` operation (e.g., record not registered, etc.).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use aimdb_core::AimDbBuilder;
    /// use aimdb_sync::{AimDbBuilderSyncExt, SyncResult};
    /// use aimdb_tokio_adapter::TokioAdapter;
    /// use std::sync::Arc;
    ///
    /// # #[derive(Debug, Clone)]
    /// # struct MyData { value: i32 }
    /// # fn main() -> SyncResult<()> {
    /// let handle = AimDbBuilder::new()
    ///     .runtime(Arc::new(TokioAdapter))
    ///     .attach()?;
    /// let producer = handle.producer::<MyData>("my_data")?;
    /// producer.set(MyData { value: 42 })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set(&self, value: T) -> SyncResult<()> {
        if let Some(_db) = self.db.upgrade() {
            self.producer.produce(value);
            Ok(())
        } else {
            Err(SyncError::RuntimeShutdown)
        }
    }
}

/// Set-by-primitive verbs for `Settable` types (feature `data-contracts`).
///
/// Where [`set`](Self::set) takes a fully constructed `T`, `set_value`
/// constructs it via `T::set(value, timestamp)` and sends, in one call.
/// Distinct from AimX's `record.set {name, value}` (full JSON value through
/// `JsonCodec`) — this is set-by-primitive.
#[cfg(feature = "data-contracts")]
impl<T> SyncProducer<T>
where
    T: aimdb_data_contracts::Settable + Send + 'static + Debug + Clone,
{
    /// Construct via `T::set(value, now)` and send. Blocking, like [`set`](Self::set).
    ///
    /// Stamps with the *caller's* `SystemTime` (sample time at the edge), not
    /// the engine's `ctx.time()` — use [`set_value_at`](Self::set_value_at) for
    /// explicit-timestamp control (replay, testing).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use aimdb_sync::SyncResult;
    /// # fn main() -> SyncResult<()> {
    /// use aimdb_core::AimDbBuilder;
    /// use aimdb_data_contracts::{SchemaType, Settable};
    /// use aimdb_sync::AimDbBuilderSyncExt;
    /// use aimdb_tokio_adapter::TokioAdapter;
    /// use std::sync::Arc;
    ///
    /// #[derive(Debug, Clone)]
    /// struct Temperature { celsius: f32, timestamp: u64 }
    ///
    /// impl SchemaType for Temperature {
    ///     const NAME: &'static str = "temperature";
    /// }
    ///
    /// impl Settable for Temperature {
    ///     type Value = f32;
    ///     fn set(value: f32, timestamp: u64) -> Self {
    ///         Temperature { celsius: value, timestamp }
    ///     }
    /// }
    ///
    /// let handle = AimDbBuilder::new().runtime(Arc::new(TokioAdapter)).attach()?;
    /// let producer = handle.producer::<Temperature>("temperature")?;
    /// producer.set_value(22.5)?; // constructs Temperature::set(22.5, now_ms) and sends
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_value(&self, value: T::Value) -> SyncResult<()> {
        self.set(T::set(value, unix_now_ms()))
    }

    /// Explicit-timestamp variant (replay, testing).
    pub fn set_value_at(&self, value: T::Value, timestamp_ms: u64) -> SyncResult<()> {
        self.set(T::set(value, timestamp_ms))
    }
}

/// Current wall-clock time as Unix milliseconds (caller-side clock).
#[cfg(feature = "data-contracts")]
fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    #[allow(dead_code)]
    fn check<X: Send + 'static + core::fmt::Debug + Clone>() {
        assert_send::<crate::SyncProducer<X>>();
        assert_sync::<crate::SyncProducer<X>>();
    }
}
