# Go SDK Cookbook

KubeMQ Go SDK Cookbook

## Get Started

### Deploy KubeMQ Cluster
#### Option 1:
Get KubeMQ Cluster with default settings - [Quick Start](https://kubemq.io/quick-start/)

#### Option 2:
Build and Deploy KubeMQ Cluster with advanced configurations - [Build & Deploy](https://build.kubemq.io/)

### Port-Forward KubeMQ Grpc Interface

Use kubectl to port-forward kubemq grpc interface 
```
kubectl port-forward svc/kubemq-cluster-grpc 50000:50000 -n kubemq
```


## Recipes

| Category             | Sub-Category             | Description                                                             |
|:---------------------|:-------------------------|:------------------------------------------------------------------------|
| Client               |                          |                                                                         |
|                      | Basic                    | Basic Client                                                            |
|                      | Connectivity             | Connectivity check upon client creation                                 |
|                      | Re-Connect               | Client with re-connect                                                  |
|                      | Authentication           | Client with JWT Authentication token                                    |
|                      | TLS                      | Client with TLS                                                         |
|                      |                          |                                                                         |
| Queues               |                          |                                                                         |
|                      | Single                   | Send/Receive single queue message                                       |
|                      | Batch                    | Send/Receive batch queue messages                                       |
|                      | Delayed                  | Send/Receive delayed queue messages                                     |
|                      | Expiration               | Send/Receive queue messages with time expiration                        |
|                      | Dead-Letter              | Send/Receive queue messages with dead-letter queue                      |
|                      | Multicast                | Send/Receive queue messages to multiple queues                          |
|                      | Multicast Mix            | Send/Receive queue messages to queues, events store and events channels |
|                      | Stream                   | Send/Stream receive queue messages with ack                             |
|                      | Stream-extend-visibility | Send/Stream receive queue messages with ack and visibility extension    |
|                      | Stream-resend            | Send/Stream receive queue messages with resend to another queue         |
|                      | Peek                     | Peek queue messages                                                     |
|                      | Ack-All                  | Ack all messages in queue                                               |
|                      |                          |                                                                         |
| Pub/Sub Events       |                          |                                                                         |
|                      | Single                   |  Send/Subscribe events messages                                                                        |
|                      | Stream                   |  Stream Send/Subscribe events messages                                                                       |
|                      | Load Balance             |  Send/Subscribe load balancing multiple receivers|
|                      | Multicast                |  Send/Subscribe to multiple events channels                                                                       |
|                      | Multicast Mix            |  Send/Subscribe events messages to queues, events-store and events channels                                                                      |
|                      | Wildcards                |  Send/Subscribe events messages with wildcard subscription|
|                      |                          |                                                                         |
| Pub/Sub Events Store |                          |                                                                         |
|                      | Single                   |  Send/Subscribe events store messages                                                                         |
|                      | Stream                   |  Stream Send/Subscribe events store messages                                                                       |
|                      | Load Balance             |   Send/Subscribe load balancing multiple receivers                                                                      |
|                      | Multicast                |   Send/Subscribe to multiple events store channels                                                                      |
|                      | Multicast Mix            |   Send/Subscribe events store messages to queues, events-store and events channels                                                                      |
|                      | Offset                   |  Send/Subscribe events store messages with offset subscription|
|                      |                          |                                                                         |
| RPC                  |                          |                                                                         |
|                      | Commands                 | Send/Subscribe rpc command messages                                                                         |
|                      | Queries                  |  Send/Subscribe rpc query messages                                                                        |
|                      |                          |                                                                         |
