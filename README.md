#Rust Redis Clone
A high-performance, multi-threaded Redis clone written in Rust, focusing on core Redis features including data structures, persistence, transactions, and replication.

This project is an implementation of some of the most essential Redis functionalities, built from the ground up to understand the core architecture of an in-memory data store.

Features Implemented
Core Commands: PING, ECHO

String Operations: SET, GET, INCR (with expiry PX)

List Operations: LPUSH, RPUSH, LPOP, LLEN, LRANGE (with negative indexes)

Blocking Operations: BLPOP with timeout support

Server Commands: INFO replication, CONFIG GET

Transactions: Full MULTI, EXEC, DISCARD support with command queuing.

Persistence: Loads database state from an .rdb file on startup.

Replication:

Can be started as a master or a replica.

Performs the full replication handshake (PING, REPLCONF, PSYNC).

Master propagates write commands to all connected replicas.

Fully multi-threaded, with each client connection handled in isolation. Shared state is managed safely with Arc<Mutex<T>>.

