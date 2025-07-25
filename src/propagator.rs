use std::sync::mpsc::Sender;

pub struct CommandPropagator {
    replicas: Vec<Sender<String>>,
}

impl CommandPropagator {
    pub fn new() -> Self {
        Self {
            replicas: Vec::new(),
        }
    }

    pub fn add_replica(&mut self, sender: Sender<String>) {
        self.replicas.push(sender);
    }

    pub fn propagate(&mut self, command: String) {
        // We don't want to block the master if a replica is slow,
        // so we just try to send. If the channel is full, we drop it.
        // We also remove senders that have been disconnected.
        self.replicas.retain(|sender| sender.send(command.clone()).is_ok());
    }
}