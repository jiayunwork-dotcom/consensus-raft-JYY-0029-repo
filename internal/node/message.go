package node

import "fmt"

// String returns a human-readable representation of a message.
func (m Message) String() string {
	switch m.Type {
	case MsgRequestVote:
		return fmt.Sprintf("[RequestVote from=%d to=%d term=%d lastIdx=%d lastTerm=%d]",
			m.From, m.To, m.Term, m.LastLogIndex, m.LastLogTerm)
	case MsgRequestVoteResp:
		return fmt.Sprintf("[RequestVoteResp from=%d to=%d term=%d granted=%v]",
			m.From, m.To, m.Term, m.Granted)
	case MsgAppendEntries:
		return fmt.Sprintf("[AppendEntries from=%d to=%d term=%d prevIdx=%d entries=%d commit=%d]",
			m.From, m.To, m.Term, m.PrevLogIndex, len(m.Entries), m.LeaderCommit)
	case MsgAppendEntriesResp:
		return fmt.Sprintf("[AppendEntriesResp from=%d to=%d term=%d success=%v match=%d]",
			m.From, m.To, m.Term, m.Success, m.MatchIndex)
	default:
		return fmt.Sprintf("[Message type=%d from=%d to=%d]", m.Type, m.From, m.To)
	}
}

// IsRequest reports whether this is a request (as opposed to a response).
func (m Message) IsRequest() bool {
	return m.Type == MsgRequestVote || m.Type == MsgAppendEntries
}

// IsResponse reports whether this is a response.
func (m Message) IsResponse() bool {
	return m.Type == MsgRequestVoteResp || m.Type == MsgAppendEntriesResp
}

// IsHeartbeat reports whether this is an empty AppendEntries (heartbeat).
func (m Message) IsHeartbeat() bool {
	return m.Type == MsgAppendEntries && len(m.Entries) == 0
}
