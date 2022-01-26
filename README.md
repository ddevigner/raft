# The Raft Consensus Algorithm
Distributed Systems University Project: "KEY/VALUE" Storage system via distributed server. Raft Consensus Algorythm in Golang. Implements:
- Leader selection.
- Log validation and replication.
- State machine application.

For more information, visit the [raft github repository]

## Execution.
Pass as many addresses as raft nodes you want and indicate which one, the current node you are trying to launch, is.
```bash
# Run it immediatly.
go run storage_srvraft.go <me> <ip/dns:port>...

# Or if you preffer, you can build and launch it.
go build storage_srvraft.go
{storage_srvraft|./storage_srvraft} <me> <ip/dns:port>...
```

Sends operation to the server indefinitely.
```bash
# Run it immediatly.
go run simple_cltraft.go <ip/dns:port>...

# Or if you preffer, you can build and launch it.
go build simple_cltraft.go 
{simple_cltraft|./simple_cltraft} <ip/dns:port>...
```

Implements several operations to interact with the raft server.
```bash
# Run it immediatly.
go run interactive_cltraft.go <ip/dns:port>...

# Or if you preffer, you can build and launch it.
go build interactive_cltraft.go
{interactive_cltraft|./interactive_cltraft} <ip/dns:port>...
```

It has already defined the ip, then for interacting with new addresses you must change them.
```bash
go test -v raft_integration_test.go 
```

## Notes
- Developed with ***go1.17.2 windows/amd64.***
- It can be compiled on both linux and windows. 
- This code does not have in count the backend about the connection system in which it can be builded on, it fully depends on your own design decision (ssh, local connections, Kubernetes-Docker).

[raft github repository]: https://raft.github.io
[the operation struct]:https://github.com/ddevigner/raft-go/blob/main/internal/raft/raft.go#L84-L88
