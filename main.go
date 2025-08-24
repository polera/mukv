package main

import (
	"time"

	mukv "github.com/polera/mukv/pkg"
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
