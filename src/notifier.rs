
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::Sender;

pub struct Notifier {
    waiters: HashMap<String, VecDeque<Sender<()>>>,
}

impl Notifier {
    pub fn new() -> Self {
        Self {
            waiters: HashMap::new(),
        }
    }

    pub fn add_waiter(&mut self, key: String, sender: Sender<()>) {
        self.waiters.entry(key).or_default().push_back(sender);
    }

    pub fn notify_waiter(&mut self, key: &str) {
        if let Some(waiters_for_key) = self.waiters.get_mut(key) {
            if let Some(sender) = waiters_for_key.pop_front() {
                // Send the signal. We don't care if it fails (the client might have disconnected).
                let _ = sender.send(());
            }
            if waiters_for_key.is_empty() {
                self.waiters.remove(key);
            }
        }
    }
}