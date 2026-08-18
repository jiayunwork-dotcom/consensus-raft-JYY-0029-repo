package node

// Timer is a logical countdown timer used for election and heartbeat timeouts.
type Timer struct {
	deadline int
	elapsed  int
	active   bool
}

// NewTimer creates a timer with the given deadline.
func NewTimer(deadline int) *Timer {
	return &Timer{deadline: deadline, active: true}
}

// Tick advances the timer by one logical tick. Returns true if the timer fired.
func (t *Timer) Tick() bool {
	if !t.active {
		return false
	}
	t.elapsed++
	return t.elapsed >= t.deadline
}

// Reset restarts the timer with a new deadline.
func (t *Timer) Reset(deadline int) {
	t.deadline = deadline
	t.elapsed = 0
	t.active = true
}

// Stop disables the timer.
func (t *Timer) Stop() {
	t.active = false
}

// IsActive reports whether the timer is running.
func (t *Timer) IsActive() bool { return t.active }

// Elapsed returns ticks elapsed since last reset.
func (t *Timer) Elapsed() int { return t.elapsed }

// Remaining returns ticks until deadline.
func (t *Timer) Remaining() int {
	r := t.deadline - t.elapsed
	if r < 0 {
		return 0
	}
	return r
}

// Fired reports whether the timer has already fired.
func (t *Timer) Fired() bool {
	return t.active && t.elapsed >= t.deadline
}
