package http

import (
	"encoding/json"
	"net/http"
)

type HealthResponse struct {
	Status     string                     `json:"status"`
	Service    string                     `json:"service"`
	Version    string                     `json:"version"`
	Components map[string]ComponentStatus `json:"components"`
}

type ComponentStatus struct {
	Status  string `json:"status"`
	Details string `json:"details,omitempty"`
}

func HealthHandler(serviceName, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		resp := HealthResponse{
			Status:  "UP",
			Service: serviceName,
			Version: version,
			Components: map[string]ComponentStatus{
				"kafka":    {Status: "UP"},
				"database": {Status: "DOWN", Details: "not configured for baseline"},
			},
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}
}
