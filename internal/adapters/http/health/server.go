package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"time"
)

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
