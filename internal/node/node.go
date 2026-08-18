// Package node implements a single Raft peer as a deterministic, tick-driven
// state machine.
//
// A Node never sends a message on its own: every message leaves through the
// return value of Step (in response to an incoming message) or Tick (when a
// logical timer fires). The caller — typically the cluster simulator or a test —
// is responsible for delivering the returned messages to their destinations.
//
// Determinism is a hard requirement. There are no goroutines, no wall-clock
// timers and no randomness except the injectable, seeded pseudo-random source
// used to pick each node's election timeout. Given the same sequence of Step /
// Tick calls and the same seed, a Node behaves identically every run.
package node

import (
	"errors"
	"fmt"
	"math/rand"

	"consensus-raft/internal/log"
)

// Errors returned by the library on bad input or illegal operations.
var (
	// ErrNotLeader is returned when Propose is called on a non-leader.
	ErrNotLeader = errors.New("node: not leader")
	// ErrNodeCrashed is returned when an operation is attempted on a crashed node.
	ErrNodeCrashed = errors.New("node: node is crashed")
)

// State is the Raft role of a node.
type State int

const (
	// StateFollower is a passive replica that replicates the leader's log.
	StateFollower State = iota
	// StateCandidate is campaigning for leadership in an election.
	StateCandidate
	// StateLeader is the elected primary that serves client commands.
	StateLeader
)

// String renders the state for logs.
func (s State) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// MessageType enumerates the Raft RPC messages.
type MessageType int

const (
	// MsgRequestVote is a candidate's vote request.
	MsgRequestVote MessageType = iota
	// MsgRequestVoteResp is a follower's vote response.
	MsgRequestVoteResp
	// MsgAppendEntries is a leader's log replication / heartbeat message.
	MsgAppendEntries
	// MsgAppendEntriesResp is a follower's replication acknowledgement.
	MsgAppendEntriesResp
)

// Message is a single Raft RPC. A single struct is used for all four message
// kinds; unused fields are simply zero for a given type.
type Message struct {
	Type MessageType
	From uint64
	To   uint64
	Term uint64

	// RequestVote payload.
	LastLogIndex uint64
	LastLogTerm  uint64

	// RequestVoteResp payload.
	Granted bool

	// AppendEntries payload.
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []log.Entry
	LeaderCommit uint64

	// AppendEntriesResp payload.
	Success    bool
	MatchIndex uint64
}

// NewRequestVote builds a vote request message.
func NewRequestVote(from, to, term, lastIndex, lastTerm uint64) Message {
	return Message{Type: MsgRequestVote, From: from, To: to, Term: term,
		LastLogIndex: lastIndex, LastLogTerm: lastTerm}
}

// NewRequestVoteResp builds a vote response message.
func NewRequestVoteResp(from, to, term uint64, granted bool) Message {
	return Message{Type: MsgRequestVoteResp, From: from, To: to, Term: term, Granted: granted}
}

// NewAppendEntries builds an AppendEntries message.
func NewAppendEntries(from, to, term, prevIndex, prevTerm, leaderCommit uint64, entries []log.Entry) Message {
	return Message{Type: MsgAppendEntries, From: from, To: to, Term: term,
		PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: leaderCommit}
}

// NewAppendEntriesResp builds an AppendEntries response message.
func NewAppendEntriesResp(from, to, term uint64, success bool, matchIndex uint64) Message {
	return Message{Type: MsgAppendEntriesResp, From: from, To: to, Term: term,
		Success: success, MatchIndex: matchIndex}
}

// RNG is the minimal random source the node needs. Supplying a deterministic,
// seeded implementation makes the whole simulation reproducible.
type RNG interface {
	Intn(n int) int
}

// NewSeededRNG returns a deterministic RNG backed by math/rand.
func NewSeededRNG(seed int64) RNG {
	return rand.New(rand.NewSource(seed))
}

// Node is one Raft peer.
type Node struct {
	id    uint64
	peers []uint64

	currentTerm uint64
	votedFor    uint64

	raftLog *log.Log

	state    State
	leaderID uint64

	rng RNG

	// Logical timers (in ticks, not wall-clock time).
	electionTimeout  int
	electionElapsed  int
	heartbeatElapsed int

	votesGranted map[uint64]bool
	nextIndex    map[uint64]uint64
	matchIndex   map[uint64]uint64

	crashed bool

	events []string

	// Tunable timing knobs (overridable by tests for speed).
	electionTimeoutMin int
	electionTimeoutMax int
	heartbeatInterval  int
}

// New constructs a follower node with the given id and peer ids. The supplied
// RNG is used to draw randomized election timeouts; pass NewSeededRNG for
// reproducible runs.
func New(id uint64, peers []uint64, rng RNG) *Node {
	peerCopy := make([]uint64, len(peers))
	copy(peerCopy, peers)
	n := &Node{
		id:                 id,
		peers:              peerCopy,
		raftLog:            log.New(),
		state:              StateFollower,
		rng:                rng,
		votesGranted:       map[uint64]bool{},
		nextIndex:          map[uint64]uint64{},
		matchIndex:         map[uint64]uint64{},
		electionTimeoutMin: 10,
		electionTimeoutMax: 20,
		heartbeatInterval:  2,
	}
	n.resetElectionTimer()
	return n
}

// ID returns the node id.
func (n *Node) ID() uint64 { return n.id }

// State returns the current role.
func (n *Node) State() State { return n.state }

// Term returns the current term.
func (n *Node) Term() uint64 { return n.currentTerm }

// LeaderID returns the id of the recognized leader (0 if none).
func (n *Node) LeaderID() uint64 { return n.leaderID }

// Crashed reports whether the node is currently crashed.
func (n *Node) Crashed() bool { return n.crashed }

// CommitIndex returns the highest committed index.
func (n *Node) CommitIndex() uint64 { return n.raftLog.CommitIndex() }

// AllEntries returns every entry currently in the log (index >= 1).
func (n *Node) AllEntries() []log.Entry {
	lastIdx, _ := n.raftLog.Last()
	return n.raftLog.Slice(1, lastIdx+1)
}

// CommittedEntries returns the entries at or below the commit index.
func (n *Node) CommittedEntries() []log.Entry { return n.raftLog.CommittedEntries() }

// RaftLog exposes the underlying replicated log.
func (n *Node) RaftLog() *log.Log { return n.raftLog }

// DrainEvents returns and clears the pending human-readable event log.
func (n *Node) DrainEvents() []string {
	ev := n.events
	n.events = nil
	return ev
}

func (n *Node) addEvent(format string, args ...interface{}) {
	n.events = append(n.events, fmt.Sprintf(format, args...))
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	span := n.electionTimeoutMax - n.electionTimeoutMin + 1
	if span < 1 {
		span = 1
	}
	n.electionTimeout = n.electionTimeoutMin + n.rng.Intn(span)
}

func (n *Node) majority() int {
	total := len(n.peers) + 1
	return total/2 + 1
}

func (n *Node) countVotes() int {
	c := 0
	if n.votesGranted[n.id] {
		c++
	}
	for _, p := range n.peers {
		if n.votesGranted[p] {
			c++
		}
	}
	return c
}

func (n *Node) isUpToDate(lastIndex, lastTerm uint64) bool {
	myIdx, myTerm := n.raftLog.Last()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= myIdx
}

// Tick advances the logical clock by one tick and returns any outbound messages
// (for example a heartbeat or a freshly started election).
func (n *Node) Tick() []Message {
	if n.crashed {
		return nil
	}
	n.electionElapsed++
	switch n.state {
	case StateFollower, StateCandidate:
		if n.electionElapsed >= n.electionTimeout {
			return n.startElection()
		}
	case StateLeader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatInterval {
			n.heartbeatElapsed = 0
			return n.broadcastAppendEntries()
		}
	}
	return nil
}

func (n *Node) startElection() []Message {
	n.currentTerm++
	n.state = StateCandidate
	n.votedFor = n.id
	n.votesGranted = map[uint64]bool{n.id: true}
	n.leaderID = 0
	n.resetElectionTimer()
	n.addEvent("node %d started election for term %d", n.id, n.currentTerm)

	lastIdx, lastTerm := n.raftLog.Last()
	var out []Message
	for _, p := range n.peers {
		out = append(out, NewRequestVote(n.id, p, n.currentTerm, lastIdx, lastTerm))
	}
	return out
}

func (n *Node) becomeLeader() []Message {
	n.state = StateLeader
	n.leaderID = n.id
	n.resetElectionTimer()
	n.heartbeatElapsed = 0
	lastIdx, _ := n.raftLog.Last()
	n.nextIndex = map[uint64]uint64{}
	n.matchIndex = map[uint64]uint64{}
	for _, p := range n.peers {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
	}
	n.addEvent("node %d became leader for term %d", n.id, n.currentTerm)
	return n.broadcastAppendEntries()
}

func (n *Node) broadcastAppendEntries() []Message {
	var out []Message
	lastIdx, _ := n.raftLog.Last()
	commit := n.raftLog.CommitIndex()
	for _, p := range n.peers {
		ni := n.nextIndex[p]
		var prevIndex, prevTerm uint64
		if ni > 1 {
			prevIndex = ni - 1
			if t, err := n.raftLog.TermAt(prevIndex); err == nil {
				prevTerm = t
			} else {
				prevIndex = 0
			}
		}
		var entries []log.Entry
		if ni <= lastIdx {
			entries = n.raftLog.Slice(ni, lastIdx+1)
		}
		out = append(out, NewAppendEntries(n.id, p, n.currentTerm, prevIndex, prevTerm, commit, entries))
	}
	return out
}

// ReplicateNow forces the leader to (re)send its log tail to every follower
// immediately, bypassing the heartbeat interval. It is used by Propose so a
// freshly appended command is pushed to the cluster without waiting for a tick.
func (n *Node) ReplicateNow() []Message {
	if n.state != StateLeader || n.crashed {
		return nil
	}
	return n.broadcastAppendEntries()
}

// Step applies an incoming message and returns the node's response(s).
func (n *Node) Step(m Message) []Message {
	if n.crashed {
		return nil
	}
	switch m.Type {
	case MsgRequestVote:
		return n.handleRequestVote(m)
	case MsgRequestVoteResp:
		return n.handleRequestVoteResp(m)
	case MsgAppendEntries:
		return n.handleAppendEntries(m)
	case MsgAppendEntriesResp:
		return n.handleAppendEntriesResp(m)
	}
	return nil
}

func (n *Node) handleRequestVote(m Message) []Message {
	if m.Term < n.currentTerm {
		return []Message{NewRequestVoteResp(n.id, m.From, n.currentTerm, false)}
	}
	if m.Term > n.currentTerm {
		n.currentTerm = m.Term
		n.votedFor = 0
		n.state = StateFollower
		n.leaderID = 0
	}
	grant := false
	if (n.votedFor == 0 || n.votedFor == m.From) && n.isUpToDate(m.LastLogIndex, m.LastLogTerm) {
		n.votedFor = m.From
		grant = true
		n.resetElectionTimer()
	}
	return []Message{NewRequestVoteResp(n.id, m.From, n.currentTerm, grant)}
}

func (n *Node) handleRequestVoteResp(m Message) []Message {
	if m.Term > n.currentTerm {
		n.stepDown(m.Term)
		return nil
	}
	if m.Term == n.currentTerm && n.state == StateCandidate {
		if m.Granted {
			n.votesGranted[m.From] = true
		}
		if n.countVotes() >= n.majority() {
			return n.becomeLeader()
		}
	}
	return nil
}

func (n *Node) handleAppendEntries(m Message) []Message {
	if m.Term < n.currentTerm {
		return []Message{NewAppendEntriesResp(n.id, m.From, n.currentTerm, false, 0)}
	}
	if m.Term >= n.currentTerm {
		if n.state != StateFollower || m.Term > n.currentTerm {
			n.currentTerm = m.Term
			n.state = StateFollower
			n.votedFor = 0
		}
		n.leaderID = m.From
		n.resetElectionTimer()
	}

	if !n.raftLog.MatchesPrevLogTerm(m.PrevLogIndex, m.PrevLogTerm) {
		lastIdx, _ := n.raftLog.Last()
		return []Message{NewAppendEntriesResp(n.id, m.From, n.currentTerm, false, lastIdx)}
	}

	if len(m.Entries) > 0 {
		ents := deepCopyEntries(m.Entries)
		if err := n.raftLog.Append(ents); err != nil {
			lastIdx, _ := n.raftLog.Last()
			return []Message{NewAppendEntriesResp(n.id, m.From, n.currentTerm, false, lastIdx)}
		}
	}

	lastIdx, _ := n.raftLog.Last()
	if m.LeaderCommit > n.raftLog.CommitIndex() {
		newCommit := m.LeaderCommit
		if newCommit > lastIdx {
			newCommit = lastIdx
		}
		if err := n.raftLog.CommitTo(newCommit); err == nil && newCommit > 0 {
			n.addEvent("node %d committed index %d (term %d)", n.id, n.raftLog.CommitIndex(), n.currentTerm)
		}
	}
	return []Message{NewAppendEntriesResp(n.id, m.From, n.currentTerm, true, lastIdx)}
}

func (n *Node) handleAppendEntriesResp(m Message) []Message {
	if m.Term > n.currentTerm {
		n.stepDown(m.Term)
		return nil
	}
	if m.Term == n.currentTerm && n.state == StateLeader {
		if m.Success {
			n.matchIndex[m.From] = m.MatchIndex
			n.nextIndex[m.From] = m.MatchIndex + 1
			n.advanceCommit()
		} else if n.nextIndex[m.From] > 1 {
			n.nextIndex[m.From]--
		}
	}
	return nil
}

// advanceCommit enforces the Raft commitment safety rule: a leader may only
// advance its commit index through an entry from its own current term. Once a
// current-term entry is replicated on a majority, every earlier entry (including
// prior-term entries) is committed along with it.
func (n *Node) advanceCommit() {
	lastIdx, _ := n.raftLog.Last()
	for idx := lastIdx; idx > n.raftLog.CommitIndex(); idx-- {
		term, err := n.raftLog.TermAt(idx)
		if err != nil {
			break
		}
		if term != n.currentTerm {
			continue
		}
		if n.replicatedOnMajority(idx) {
			if err := n.raftLog.CommitTo(idx); err == nil {
				n.addEvent("node %d committed index %d (term %d)", n.id, idx, n.currentTerm)
			}
			return
		}
	}
}

func (n *Node) replicatedOnMajority(idx uint64) bool {
	c := 1 // the leader itself holds every entry
	for _, p := range n.peers {
		if n.matchIndex[p] >= idx {
			c++
		}
	}
	return c >= n.majority()
}

func (n *Node) stepDown(term uint64) {
	n.currentTerm = term
	n.state = StateFollower
	n.votedFor = 0
	n.leaderID = 0
	n.votesGranted = map[uint64]bool{}
	n.nextIndex = map[uint64]uint64{}
	n.matchIndex = map[uint64]uint64{}
	n.resetElectionTimer()
	n.addEvent("node %d stepped down to follower (term %d)", n.id, n.currentTerm)
}

// Propose appends a new command to the leader's log. It returns ErrNotLeader if
// the node is not currently the leader.
func (n *Node) Propose(cmd []byte) error {
	if n.crashed {
		return ErrNodeCrashed
	}
	if n.state != StateLeader {
		return ErrNotLeader
	}
	lastIdx, _ := n.raftLog.Last()
	e := log.Entry{Term: n.currentTerm, Index: lastIdx + 1, Command: append([]byte(nil), cmd...)}
	if err := n.raftLog.Append([]log.Entry{e}); err != nil {
		return err
	}
	n.addEvent("node %d proposed command at index %d (term %d)", n.id, e.Index, n.currentTerm)
	return nil
}

// Crash simulates a power loss: the node stops participating and forgets all
// volatile state, but its persisted state (term, vote, log) survives.
func (n *Node) Crash() error {
	if n.crashed {
		return nil
	}
	n.crashed = true
	n.state = StateFollower
	n.leaderID = 0
	n.votesGranted = map[uint64]bool{}
	n.nextIndex = map[uint64]uint64{}
	n.matchIndex = map[uint64]uint64{}
	n.electionElapsed = 0
	return nil
}

// Restart simulates a reboot: only the persisted state (term, vote, entries)
// survives; all volatile state is reinitialized. This exercises the persistent
// state serialization round-trip.
func (n *Node) Restart() error {
	if !n.crashed {
		return nil
	}
	state := log.PersistedState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Entries:     n.AllEntries(),
	}
	data := log.Serialize(state)
	restored, err := log.Deserialize(data)
	if err != nil {
		return err
	}
	n.raftLog = log.New()
	if err := n.raftLog.Append(restored.Entries); err != nil {
		return err
	}
	n.currentTerm = restored.CurrentTerm
	n.votedFor = restored.VotedFor
	n.state = StateFollower
	n.leaderID = 0
	n.votesGranted = map[uint64]bool{}
	n.nextIndex = map[uint64]uint64{}
	n.matchIndex = map[uint64]uint64{}
	n.crashed = false
	n.electionElapsed = 0
	n.resetElectionTimer()
	n.addEvent("node %d restarted (term %d, log length %d)", n.id, n.currentTerm, n.raftLog.Len())
	return nil
}

// Persist returns the serialized persistent state of the node.
func (n *Node) Persist() []byte {
	return log.Serialize(log.PersistedState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Entries:     n.AllEntries(),
	})
}

func deepCopyEntries(in []log.Entry) []log.Entry {
	out := make([]log.Entry, len(in))
	for i, e := range in {
		cmd := make([]byte, len(e.Command))
		copy(cmd, e.Command)
		out[i] = log.Entry{Term: e.Term, Index: e.Index, Command: cmd}
	}
	return out
}
