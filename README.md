# High-Performance Blockchain Cache (Powered by a Rust Core)
An intelligent, high-performance caching server designed to dramatically accelerate blockchain applications by reducing latency and offloading read requests from slow, expensive nodes.

This project implements a multi-threaded, Redis-compatible core in Rust to solve a real-world performance bottleneck for high-traffic dApps.

**The Problem**
Querying data directly from a blockchain is inherently slow and costly. For high-traffic dApps, NFT marketplaces, and DeFi platforms, this latency creates a poor user experience and becomes a major scalability bottleneck.

This project solves that problem by providing a smart, in-memory caching layer that sits between the user and the blockchain, delivering instant responses for frequently accessed data.

**Key Performance Indicator (KPI)**
Our primary metric for success is the Cache Hit Ratio, which measures the percentage of requests served directly from the cache.

Initial project success will be defined by achieving a >90% cache hit ratio under production load while securing an established blockchain firm as our first Lighthouse Customer to validate the solution.

**Target Audience**
This project is designed for development teams building high-performance Web3 applications, including:

NFT Marketplaces (e.g., OpenSea, Magic Eden)

Decentralized Exchanges (DEXs) (e.g., Uniswap, Aave)

Blockchain Explorers (e.g., Etherscan, Solscan)

Web3 Gaming Platforms

**Features Implemented**
Core Caching Engine:

String Operations: SET, GET, INCR with PX expiry for managing cache entries.

List Operations: LPUSH, RPUSH, LPOP, LLEN, LRANGE for caching ordered data like transaction histories or activity feeds.

Blocking Operations: BLPOP with timeout support for building real-time data pipelines.

System Architecture:

Concurrency: Fully multi-threaded, with each client connection handled in isolation. Shared state is managed safely with Arc<Mutex<T>>.

Persistence: Loads database state from an .rdb file on startup to enable fast cache warming.

Transactions: Full MULTI, EXEC, DISCARD support for atomic operations.

High Availability & Scaling:

Replication: Can be deployed in a leader-follower configuration.

Performs the full replication handshake (PING, REPLCONF, PSYNC).

Master propagates all write commands to connected replicas to ensure cache consistency.
