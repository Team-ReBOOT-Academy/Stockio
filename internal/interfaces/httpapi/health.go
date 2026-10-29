package httpapi

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/Team-ReBOOT-Academy/Stockio.git/internal/infrastructure/config"
)

type KafkaHealth struct {
	status atomic.Value
}

func NewKafkaHealth() *KafkaHealth {
	h := &KafkaHealth{}
	h.Set("DOWN", "connection unavailable")
	return h
}

func (h *KafkaHealth) Set(status, details string) {
	h.status.Store(map[string]string{"status": status, "details": details})
}

func RegisterRoutes(mux *http.ServeMux, cfg config.Config, health *KafkaHealth) {
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		status := "UP"
		components := map[string]interface{}{}
		if health != nil {
			component := health.status.Load().(map[string]string)
			components["kafka"] = component
			if component["status"] != "UP" {
				status = "DEGRADED"
			}
		}
		response := map[string]interface{}{"status": status, "service": cfg.ServiceName, "version": cfg.AppVersion, "components": components}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	})
}
