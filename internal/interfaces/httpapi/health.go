package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/Team-ReBOOT-Academy/Stockio.git/internal/infrastructure/config"
)

func RegisterRoutes(
	mux *http.ServeMux,
	cfg config.Config,
) {
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		response := map[string]interface{}{
			"status":     "UP",
			"service":    cfg.AppName,
			"version":    cfg.AppVersion,
			"components": map[string]string{},
		}
		w.WriteHeader(http.StatusOK)

		if err := json.NewEncoder(w).Encode(response); err != nil {
			return
		}
	})
}
