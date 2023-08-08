# KV Raft 

A fault-tolerant key/value store in Go with put, append, and get operations with the Raft consensus algorithm

Course Website: https://pdos.csail.mit.edu/6.824/labs/lab-raft.html 
 
## Implementation

- [x] Lab 2: Raft Consensus Algorithm
  - [x] Lab 2A: Raft Leader Election
  - [x] Lab 2B: Raft Log Entries Append
  - [x] Lab 2C: Raft state persistence
  
- [x] Lab 3: Fault-tolerant Key/Value Service
  - [x] Lab 3A: Key/value Service Without Log Compaction
  - [x] Lab 3B: Key/value Service With Log Compaction

- [x] Lab 4: Sharded Key/Value Service
  - [x] Lab 4A: The Shard Master
  - [ ] Lab 4B: Sharded Key/Value Server


## Test

**Lab 2** 

```bash
cd src/raft
go test
```

**Lab 3**

```bash
cd src/kvraft
go test
```

**Lab 4A**

```bash
cd src/shardctrler
go test
```
