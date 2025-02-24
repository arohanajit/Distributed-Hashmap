# Service Discovery in Distributed Hashmap

This document describes the service discovery mechanisms implemented in the Distributed Hashmap project.

## Overview

Service discovery is a critical component in distributed systems, allowing nodes to find and communicate with each other dynamically. The Distributed Hashmap supports two service discovery mechanisms:

1. **Centralized Discovery**: Using etcd/Consul as a service registry
2. **Decentralized Discovery**: Using the SWIM gossip protocol

Both mechanisms support dynamic cluster membership, failure detection, and eventual consistency.

## Centralized Discovery with etcd/Consul

The centralized discovery mechanism uses etcd or Consul as a service registry.

### How It Works

1. Nodes register themselves with the service registry on startup using a time-limited lease.
2. Nodes keep their registration active by renewing their lease periodically.
3. Nodes watch the registry for changes to the node list.
4. If a node fails to renew its lease, it is automatically removed from the registry.

### Configuration

Example configuration for etcd-based discovery:

```go
config := DiscoveryConfig{
    EtcdEndpoints: []string{"localhost:2379"},
    ServicePrefix: "/services/hashmap/nodes/",
    LeaseTTL:      30, // 30 seconds
}
```

### Running with etcd

```bash
# Start etcd (example using Docker)
docker run -d --name etcd-server \
  --publish 2379:2379 \
  --env ALLOW_NONE_AUTHENTICATION=yes \
  --env ETCD_ADVERTISE_CLIENT_URLS=http://etcd-server:2379 \
  gcr.io/etcd-development/etcd:v3.5.0

# Start the server with etcd discovery
./build/dhashmap-server -node-id "node1" -discovery "etcd" -etcd-endpoints "localhost:2379"
```

## Decentralized Discovery with SWIM Gossip

The SWIM (Scalable Weakly-consistent Infection-style Process Group Membership) protocol is a gossip-based protocol for service discovery.

### How It Works

1. Nodes maintain a membership list of other nodes in the cluster.
2. Nodes periodically select a random node from their list and send a ping.
3. If a node doesn't respond to a direct ping, indirect pings are sent through other nodes.
4. Membership updates are piggy-backed on regular messages.
5. Failed nodes are marked as suspicious before being declared dead.

### Configuration

Example configuration for SWIM-based discovery:

```go
config := SwimConfig{
    UDPPort:        7946,
    TCPPort:        7947,
    SuspectTimeout: 5 * time.Second,
    ProtocolPeriod: 1 * time.Second,
    IndirectNodes:  3,
    MaxBroadcasts:  3,
}
```

### Running with SWIM

```bash
# Start the first node
./build/dhashmap-server -node-id "node1" -discovery "swim"

# Join an existing cluster with a seed node
./build/dhashmap-server -node-id "node2" -discovery "swim" -seed-nodes "node1:7946"
```

## Failure Detection

Both discovery mechanisms include failure detection:

- **Health Checks**: Regular health checks to detect failed nodes
- **Suspicion Mechanism**: Nodes are marked as suspicious before being declared dead
- **Eventual Consistency**: The system eventually converges to a consistent view of membership

### etcd Failure Detection

In etcd-based discovery, failure detection is built into the lease mechanism:

1. Nodes acquire a lease with a Time-To-Live (TTL)
2. Nodes must periodically renew their lease
3. If a node fails to renew its lease, it is automatically removed from the registry

### SWIM Failure Detection

The SWIM protocol includes a failure detector:

1. Direct Ping: Send a ping directly to a random node
2. Indirect Ping: If direct ping fails, ask other nodes to ping the target
3. Suspicion: Mark unresponsive nodes as suspicious
4. Confirmation: Declare nodes dead after a timeout
5. Refutation: Allow nodes to refute their "dead" status

## Integration with Cluster Management

The discovery mechanisms are integrated with the cluster management system:

1. When a node joins/leaves the cluster, the appropriate cluster management operations are triggered
2. Failure detection events are propagated to the cluster manager
3. The cluster manager updates the consistent hash ring and may trigger re-replication of data

## Eventual Consistency Guarantees

Both discovery mechanisms provide eventual consistency guarantees:

- Temporary discrepancies in node lists are allowed during convergence
- The system eventually reaches a consistent view of cluster membership
- Data remains available during membership changes

## Best Practices

1. For smaller clusters, SWIM gossip protocol is simpler to set up and doesn't require external dependencies
2. For larger clusters, etcd/Consul provides better scalability and management features
3. Configure appropriate timeouts based on your network characteristics
4. Use redundancy in etcd/Consul setup for production environments
5. Consider network partitions in your deployment strategy 