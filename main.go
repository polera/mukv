package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/polera/mukv/mukv"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func startServer(port int) {
	listenAddr := fmt.Sprintf(":%d", port)

	zerolog.TimeFieldFormat = time.RFC3339
	logger := log.With().Str("mukv", "server").Logger()

	muKV := mukv.New(logger)

	go muKV.StartExpireLoop()

	logger.Info().Str("listenAddr", listenAddr).Msg("listening")
	err := redcon.ListenAndServe(listenAddr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError(fmt.Sprintf("ERR unknown command %s", cmd.Args[0]))
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				err := conn.Close()
				if err != nil {
					logger.Fatal().Err(err).Msg("failed to close connection")
					return
				}
			case "set":
				argLen := len(cmd.Args)
				if argLen < 3 {
					errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
					conn.WriteError(errStr)
				} else {
					var err error
					var rec *mukv.Record
					switch argLen {
					case 3:
						rec, err = muKV.Receive(string(cmd.Args[1]), "", "")
					case 5:
						switch strings.ToLower(string(cmd.Args[3])) {
						default:
							rec, err = muKV.Receive(string(cmd.Args[1]), string(cmd.Args[4]), "s")
						case "px":
							rec, err = muKV.Receive(string(cmd.Args[1]), string(cmd.Args[4]), "ms")
						}
					}
					if err != nil {
						conn.WriteError(err.Error())
					} else {
						muKV.Datastore.Store(rec.Key, cmd.Args[2])
						muKV.RWMutex.Lock()
						muKV.Records[rec.Key] = rec
						muKV.RWMutex.Unlock()
						conn.WriteString("OK")
					}

				}
			case "get":
				if len(cmd.Args) != 2 {
					errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
					conn.WriteError(errStr)
				} else {
					rec, ok := muKV.Datastore.Load(string(cmd.Args[1]))
					if !ok {
						conn.WriteNull()
					} else {
						muKV.RWMutex.Lock()
						r := muKV.Records[string(cmd.Args[1])]
						r.Hits += 1
						muKV.RWMutex.Unlock()
						logger.Debug().Int(r.Key, r.Hits).Msg("key get")
						conn.WriteAny(rec)
					}
				}
			case "ttl":
				if len(cmd.Args) != 2 {
					errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
					conn.WriteError(errStr)
				} else {
					_, ok := muKV.Datastore.Load(string(cmd.Args[1]))
					if !ok {
						conn.WriteNull()
					} else {
						muKV.RWMutex.RLock()
						r := muKV.Records[string(cmd.Args[1])]
						muKV.RWMutex.RUnlock()
						conn.WriteInt(int(r.TimeToExpiry()))
					}
				}
			case "touch":
				if len(cmd.Args) != 2 {
					errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
					conn.WriteError(errStr)
				} else {
					_, ok := muKV.Datastore.Load(string(cmd.Args[1]))
					if !ok {
						conn.WriteNull()
					} else {
						muKV.RWMutex.Lock()
						r := muKV.Records[string(cmd.Args[1])]
						r.Touch()
						muKV.RWMutex.Unlock()
						logger.Debug().Int(r.Key, r.Hits).Msg("key touch")
						conn.WriteInt(r.Hits)
					}
				}
			case "del":
				if len(cmd.Args) != 2 {
					errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
					conn.WriteError(errStr)
				} else {
					muKV.Datastore.Delete(string(cmd.Args[1]))
					muKV.RWMutex.Lock()
					_, ok := muKV.Records[string(cmd.Args[1])]
					delete(muKV.Records, string(cmd.Args[1]))
					muKV.RWMutex.Unlock()
					if !ok {
						conn.WriteInt(0)
					} else {
						conn.WriteInt(1)
					}
				}

			}
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {

		},
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to start server")
	}
}

func main() {
	startServer(6480)
}
