pub use tokio::sync::broadcast::error::TryRecvError;
pub use tokio::sync::broadcast::error::RecvError;
pub use tokio::sync::broadcast::error::SendError;

use core::sync::atomic::Ordering;
use core::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::broadcast::{Sender as TokioSender, Receiver as TokioReceiver};

#[derive(Clone)]
pub struct Sender<T> {
    tx: TokioSender<T>,
    fanout: u8,
    fanout_tx: Vec<TokioSender<T>>,
    rx_idx: Arc<AtomicUsize>,
}

impl<T> Sender<T> {
    pub fn send(&self, item: T) -> Result<usize, SendError<T>>{
        self.tx.send(item)
    }

    pub fn subscribe(&self) -> Receiver<T> {
        if self.fanout == 0 {
            Receiver {
                rx: self.tx.subscribe(),
            }
        } else {
            let idx = self.rx_idx.fetch_add(1, Ordering::Relaxed) & ((1 << self.fanout) - 1);
            let rx = self.fanout_tx[idx].subscribe();

            Receiver {
                rx,
            }
        }
    }
}

pub struct Receiver<T> {
    rx: TokioReceiver<T>,
}

impl<T: Clone> Receiver<T> {
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.rx.try_recv()
    }
}

struct Fanout<T> {
    idx: usize,
    rx: TokioReceiver<T>,
    tx: Vec<TokioSender<T>>,
}

pub fn channel<T: 'static + Clone + Send>(runtime: &tokio::runtime::Runtime, capacity: usize, fanout: u8) -> Sender<T> {
    if fanout == 0 {
        let (tx, _) = tokio::sync::broadcast::channel::<T>(capacity);
        let sender = Sender {
            tx,
            fanout,
            fanout_tx: Vec::new(),
            rx_idx: Arc::new(AtomicUsize::new(0)),
        };

        return sender;
    }

    let _guard = runtime.enter();

    let (sender_tx, _) = tokio::sync::broadcast::channel::<T>(capacity);

    let mut fanout_tx = Vec::new();
    let mut receiver_rx = Vec::new();

    for _ in 0..(1 << fanout) {
        let (tx, rx) = tokio::sync::broadcast::channel::<T>(capacity);

        fanout_tx.push(tx);
        receiver_rx.push(rx);
    }

    let mut fanouts = Vec::new();

    for idx in 0..(1 << fanout) {
        fanouts.push( Fanout {
            idx,
            rx: sender_tx.subscribe(),
            tx: fanout_tx.clone(),
        })
    }

    let sender = Sender {
        tx: sender_tx,
        fanout,
        fanout_tx: fanout_tx.clone(),
        rx_idx: Arc::new(AtomicUsize::new(0)),
    };

    for mut fanout in fanouts.drain(..) {
        tokio::spawn(async move {
            loop {
                if let Ok(item) = fanout.rx.recv().await {
                    let _ = fanout.tx[fanout.idx].send(item.clone());
                }
            }
        });
    }

    sender
}

