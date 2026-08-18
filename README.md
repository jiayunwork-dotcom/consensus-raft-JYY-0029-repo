# consensus-raft

consensus-raft is a deterministic, dependency-free implementation of the Raft
leader-election and log-replication core. It is modeled as a pure, tick-driven
state machine: every node advances only when the caller invokes `Tick()`, and
every message is exchanged through an in-memory bus that a driver steps one
round at a time. Because the only source of randomness (the randomized election
timeout) is an injectable, seeded pseudo-random generator, the whole cluster is
fully reproducible — running the same scenario twice with the same seed yields
byte-for-byte identical output, with no goroutines, no wall-clock timers, and no
network flakiness.

The package is intended for studying and testing the Raft protocol properties in
isolation:

- **Leader election** — followers time out and become candidates, run a
  randomized election, and a single node is elected leader per term.
- **Log replication** — the leader appends proposed commands and replicates them
  to a majority; entries are committed once a majority has stored them.
- **Safety** — the "only commit an entry from the current term" rule prevents a
  leader from committing a prior-term entry that could later be overwritten, and
  the log-matching property guarantees identical committed prefixes across nodes.

## Packages

- `internal/log` — the replicated log: append with conflict resolution, term
  lookup, truncation, the log-matching property, monotonic commit advancement,
  and persistent-state (term / vote / entries) serialization.
- `internal/node` — one Raft peer as a deterministic state machine: `Step`
  consumes a message and returns outbound messages, `Tick` advances the logical
  election/heartbeat timers, and `Propose` accepts client commands on the
  leader only.
- `internal/cluster` — a deterministic in-memory simulator that drives a set of
  nodes with `TickAll`, network partitions, crashes and restarts, and asserts
  the core Raft invariants.

## Building

```sh
export GOTOOLCHAIN=local CGO_ENABLED=0
go build ./...
```

## Testing

```sh
export GOTOOLCHAIN=local CGO_ENABLED=0
go test -count=1 ./...
go test -race ./...
go vet ./...
```

All tests are deterministic and require no network or filesystem access.

## Running the simulator

```sh
go run . --nodes 5 --seed 42 --script example/scenario.txt
```

The CLI runs a 3- or 5-node cluster for a scripted scenario. The scenario file
is a sequence of lines:

```
tick 20
propose x=1
partition n2
heal n2
crash n3
restart n3
```

Each `tick N` advances the logical clock `N` steps; `propose` submits a command
to the current leader; `partition`/`heal` and `crash`/`restart` manipulate the
network and node lifecycles. The simulator prints the transition log and the
final replicated log of each node.
