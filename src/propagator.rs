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
        
        self.replicas
            .retain(|sender| sender.send(command.clone()).is_ok());
    }
}