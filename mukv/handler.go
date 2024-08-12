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
			var rec *Record
			switch argLen {
			case 3:
				rec, err = mkv.Receive(string(cmd.Args[1]), "", "")
			case 5:
				switch strings.ToLower(string(cmd.Args[3])) {
				default:
					rec, err = mkv.Receive(string(cmd.Args[1]), string(cmd.Args[4]), "s")
				case "px":
					rec, err = mkv.Receive(string(cmd.Args[1]), string(cmd.Args[4]), "ms")
				}
			}
			if err != nil {
				conn.WriteError(err.Error())
			} else {
				mkv.Datastore.Store(rec.Key, cmd.Args[2])
				mkv.RWMutex.Lock()
				mkv.Records[rec.Key] = rec
				mkv.RWMutex.Unlock()
				conn.WriteString("OK")
			}

		}
	case "get":
		if len(cmd.Args) != 2 {
			errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
			conn.WriteError(errStr)
		} else {
			rec, ok := mkv.Datastore.Load(string(cmd.Args[1]))
			if !ok {
				conn.WriteNull()
			} else {
				mkv.RWMutex.Lock()
				r := mkv.Records[string(cmd.Args[1])]
				r.Hits += 1
				mkv.RWMutex.Unlock()
				logger.Debug().Int(r.Key, r.Hits).Msg("key get")
				conn.WriteAny(rec)
			}
		}
	case "ttl":
		if len(cmd.Args) != 2 {
			errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
			conn.WriteError(errStr)
		} else {
			_, ok := mkv.Datastore.Load(string(cmd.Args[1]))
			if !ok {
				conn.WriteNull()
			} else {
				mkv.RWMutex.RLock()
				r := mkv.Records[string(cmd.Args[1])]
				mkv.RWMutex.RUnlock()
				conn.WriteInt(int(r.TimeToExpiry()))
			}
		}
	case "touch":
		if len(cmd.Args) != 2 {
			errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
			conn.WriteError(errStr)
		} else {
			_, ok := mkv.Datastore.Load(string(cmd.Args[1]))
			if !ok {
				conn.WriteNull()
			} else {
				mkv.RWMutex.Lock()
				r := mkv.Records[string(cmd.Args[1])]
				r.Touch()
				mkv.RWMutex.Unlock()
				logger.Debug().Int(r.Key, r.Hits).Msg("key touch")
				conn.WriteInt(r.Hits)
			}
		}
	case "del":
		if len(cmd.Args) != 2 {
			errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
			conn.WriteError(errStr)
		} else {
			mkv.Datastore.Delete(string(cmd.Args[1]))
			mkv.RWMutex.Lock()
			_, ok := mkv.Records[string(cmd.Args[1])]
			delete(mkv.Records, string(cmd.Args[1]))
			mkv.RWMutex.Unlock()
			if !ok {
				conn.WriteInt(0)
			} else {
				conn.WriteInt(1)
			}
		}
	}
}

func (mkv *MuKV) HandleAccept(conn redcon.Conn) bool {
	return true
}

func (mkv *MuKV) HandleClose(conn redcon.Conn, err error) {

}

func (mkv *MuKV) ListenAndServe(port int) error {
	listenAddr := fmt.Sprintf(":%d", port)

	logger := mkv.Log.With().Str("function", "ListenAndServe").Logger()

	go mkv.StartExpireLoop()

	logger.Info().Str("listenAddr", listenAddr).Msg("listening")
	return redcon.ListenAndServe(listenAddr,
		mkv.Handler,
		mkv.HandleAccept,
		mkv.HandleClose,
	)
}
