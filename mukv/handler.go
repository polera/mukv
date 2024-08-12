package mukv

import (
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
)

func (mkv *MuKV) Handler(conn redcon.Conn, cmd redcon.Command) {
	logger := mkv.Log.With().Str("function", "Handler").Logger()

	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		mkv.handleUnknown(conn, cmd.Args[0])
	case "ping":
		mkv.handlePing(conn)
	case "quit":
		err := mkv.handleQuit(conn)
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to close connection")
			return
		}
	case "set":
		mkv.handleSet(conn, cmd)
	case "get":
		mkv.handleGet(conn, cmd)
	case "ttl":
		mkv.handleTTL(conn, cmd)
	case "touch":
		mkv.handleTouch(conn, cmd)
	case "del":
		mkv.handleDel(conn, cmd)
	}
}

func (mkv *MuKV) HandleAccept(conn redcon.Conn) bool {
	return true
}

func (mkv *MuKV) HandleClose(conn redcon.Conn, err error) {}

func (mkv *MuKV) ListenAndServe(port int) error {
	logger := mkv.Log.With().Str("function", "ListenAndServe").Logger()

	listenAddr := fmt.Sprintf(":%d", port)
	go mkv.StartExpireLoop()
	logger.Info().Str("listenAddr", listenAddr).Msg("listening")
	return redcon.ListenAndServe(listenAddr,
		mkv.Handler,
		mkv.HandleAccept,
		mkv.HandleClose,
	)
}
