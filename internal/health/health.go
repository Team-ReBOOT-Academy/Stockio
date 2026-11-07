package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

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

func (t *Tracker) snapshot() map[string]ComponentState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]ComponentState, len(t.components))
	for k, v := range t.components {
		out[k] = v
	}
	return out
}

type Server struct {
	srv     *http.Server
	tracker *Tracker
	service string
	version string
	logger  *slog.Logger
}

func New(addr, service, version string, logger *slog.Logger) (*Server, *Tracker) {
	tracker := NewTracker()
	s := &Server{tracker: tracker, service: service, version: version, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handle)
	s.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s, tracker
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return err
	}
	s.logger.Info("http server listening", "addr", s.srv.Addr)
	err = s.srv.Serve(listener)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	comps := s.tracker.snapshot()
	overall := "UP"
	for _, c := range comps {
		if c.Status != StateUp {
			overall = "DEGRADED"
			break
		}
	}
	body := Response{Status: overall, Service: s.service, Version: s.version, Components: comps}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}
