// In src/notifier.rs
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::Sender;

pub struct Notifier {
    // The key is the list key (e.g., "list_key")
    // The value is a queue of senders. Each sender is a "wakeup call"
    // for a blocked client. We send a `()` to signal the wakeup.
    waiters: HashMap<String, VecDeque<Sender<()>>>,
}

impl Notifier {
    pub fn new() -> Self {
        Self {
            waiters: HashMap::new(),
        }
    }

    // Add a client to the waiting list for a specific key
    pub fn add_waiter(&mut self, key: String, sender: Sender<()>) {
        self.waiters.entry(key).or_default().push_back(sender);
    }

    // Wake up the next client waiting for a specific key
    pub fn notify_waiter(&mut self, key: &str) {
        if let Some(waiters_for_key) = self.waiters.get_mut(key) {
            if let Some(sender) = waiters_for_key.pop_front() {
                // Send the signal. We don't care if it fails (client might have disconnected).
                let _ = sender.send(());
            }
            // If the queue is now empty, remove it from the map
            if waiters_for_key.is_empty() {
                self.waiters.remove(key);
            }
        }
    }
}