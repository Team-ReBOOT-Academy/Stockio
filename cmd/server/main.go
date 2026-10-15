package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Team-ReBOOT-Academy/Stockio.git/internal/infrastructure/config"
	"github.com/Team-ReBOOT-Academy/Stockio.git/internal/interfaces/httpapi"
)

func main() {
	cfg := config.Load()

	mux := http.NewServeMux()

	httpapi.RegisterRoutes(mux, cfg)

	server := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}
	log.Printf("Starting %s v%s on port %s", cfg.AppName, cfg.AppVersion, cfg.HTTPPort)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Http server error %w", err)
		}
	}()
	stop := make(chan os.Signal, 1)

	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	log.Println("Server exiting")
}
