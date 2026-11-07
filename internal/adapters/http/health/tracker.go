package health

import "sync"

type State string

const (
	StateUp      State = "UP"
	StateDown    State = "DOWN"
	StateUnknown State = "UNKNOWN"
)

type ComponentState struct {
	Status  State  `json:"status"`
	Details string `json:"details,omitempty"`
}

type Response struct {
	Status     string                    `json:"status"`
	Service    string                    `json:"service"`
	Version    string                    `json:"version"`
	Components map[string]ComponentState `json:"components"`
}

type Tracker struct {
	mu         sync.RWMutex
	components map[string]ComponentState
}

func NewTracker() *Tracker {
	return &Tracker{components: make(map[string]ComponentState)}
}

func (t *Tracker) Set(name string, state State, details string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.components[name] = ComponentState{Status: state, Details: details}
}

func (t *Tracker) SetKafka(up bool, details string) {
	state := StateDown
	if up {
		state = StateUp
	}
	t.Set("kafka", state, details)
}

func (t *Tracker) snapshot() map[string]ComponentState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]ComponentState, len(t.components))
	for name, state := range t.components {
		out[name] = state
	}
	return out
}
