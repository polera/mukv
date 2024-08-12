package main

import (
	"time"

	"github.com/polera/mukv/mukv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	zerolog.TimeFieldFormat = time.RFC3339
	logger := log.With().Str("mukv", "main").Logger()
	muKV := mukv.New(logger)

	err := muKV.ListenAndServe(6480)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to start server")
	}
}
