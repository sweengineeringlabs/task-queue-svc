//! [`InMemoryTaskQueue`] — tokio mpsc channel backed task queue.

use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::sync::{mpsc, Mutex};

use task_queue_pattern::{QueueError, Task, TaskHandle, TaskQueue, MAX_TASK_PAYLOAD_BYTES};

/// Maximum number of in-flight tasks the default in-memory queue accepts.
const MAX_QUEUE_DEPTH: usize = 16_384;

/// In-memory work queue backed by [`tokio::sync::mpsc`].
///
/// Tasks are enqueued into a bounded MPSC channel. Each dequeue call retrieves
/// the next available task. Ack signals permanent removal; nack can signal redelivery.
#[derive(Clone)]
pub struct InMemoryTaskQueue {
    tx: Arc<mpsc::Sender<Task>>,
    rx: Arc<Mutex<mpsc::Receiver<Task>>>,
}

impl InMemoryTaskQueue {
    /// Construct a fresh in-memory task queue with the default channel capacity.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(MAX_QUEUE_DEPTH);
        Self {
            tx: Arc::new(tx),
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}

impl Default for InMemoryTaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskQueue for InMemoryTaskQueue {
    fn enqueue(&self, task: Task) -> impl Future<Output = Result<(), QueueError>> + Send + '_ {
        let tx = Arc::clone(&self.tx);
        async move {
            if task.payload.len() > MAX_TASK_PAYLOAD_BYTES {
                return Err(QueueError::Enqueue(format!(
                    "payload exceeds maximum size of {MAX_TASK_PAYLOAD_BYTES} bytes"
                )));
            }
            tx.send(task)
                .await
                .map_err(|e| QueueError::Enqueue(e.to_string()))
        }
    }

    fn dequeue(&self) -> impl Future<Output = Result<Option<TaskHandle>, QueueError>> + Send + '_ {
        let rx = Arc::clone(&self.rx);
        let tx = Arc::clone(&self.tx);
        async move {
            let mut guard = rx.lock().await;
            match guard.recv().await {
                Some(task) => {
                    // Cloned before the task's fields are moved into the handle
                    // below, so `nack` can re-enqueue an identical copy.
                    let requeue_task = task.clone();
                    let ack: BoxFuture<'static, Result<(), QueueError>> =
                        Box::pin(async { Ok(()) });
                    // Returns the task to the queue for redelivery, per the
                    // `TaskQueue::nack` contract.
                    let nack: BoxFuture<'static, Result<(), QueueError>> = Box::pin(async move {
                        tx.send(requeue_task)
                            .await
                            .map_err(|e| QueueError::Dequeue(e.to_string()))
                    });
                    Ok(Some(TaskHandle::new(
                        task.id,
                        task.payload,
                        task.headers,
                        ack,
                        nack,
                    )))
                }
                None => Ok(None),
            }
        }
    }

    async fn health_check(&self) -> Result<(), QueueError> {
        Ok(())
    }
}
