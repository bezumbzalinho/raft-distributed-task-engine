# Fault-Tolerant Distributed Task Execution Engine

A multi-node distributed processing system built in Java 19 using RMI and UDP Multicast, implementing Raft consensus for replicated storage and quorum-based coordinator failover.

## Architecture

The system decouples request routing, task scheduling, execution, and state persistence across five distinct node roles:

- **Client:** Submits compute jobs and queries execution results.
- **Load Balancer:** Routes incoming client workloads across active execution nodes.
- **Coordinator:** Manages task scheduling and dispatches work to available processors via thread-safe FIFO queues.
- **Worker Processors:** Execute assigned tasks concurrently and monitor coordinator health via majority-vote quorum consensus.
- **Replicated Storage Cluster:** Persists task states and outputs using a Raft-inspired leader election and log replication protocol.

## Key Engineering Highlights

- **Consensus & Leader Election:** Implemented Raft leader election and state replication across storage nodes to ensure consistency under node crashes.
- **Quorum Failure Detection:** Workers run majority-vote health checks to detect coordinator failures and trigger automatic failover without single points of failure.
- **Consistent-Hash Task Recovery:** Pending tasks are mapped via consistent hashing so surviving nodes deterministically reclaim and re-execute orphaned jobs.
- **Thread-Safe Concurrency:** Built synchronized FIFO task queues using explicit Java concurrency locks to prevent race conditions under parallel worker polling.

## Tech Stack

- **Language:** Java 19
- **Networking & RPC:** Java RMI, UDP Multicast
- **Algorithms:** Raft Consensus, Consistent Hashing, Quorum Voting

## Running Locally

```bash
# 1. Compile all node modules
javac -d bin src/**/*.java

# 2. Start Storage Cluster nodes (minimum 3 for quorum)
java -cp bin storage.StorageNode --id 1 --port 5001
java -cp bin storage.StorageNode --id 2 --port 5002
java -cp bin storage.StorageNode --id 3 --port 5003

# 3. Start Coordinator, Workers, and Load Balancer
java -cp bin coordinator.CoordinatorNode
java -cp bin worker.WorkerNode --id 1
java -cp bin lb.LoadBalancerNode

# 4. Submit a sample workload via Client
java -cp bin client.ClientRunner --tasks 20
