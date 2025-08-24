package mukv

import (
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
)

func (mkv *MuKV) handleUnknown(conn redcon.Conn, cmd []byte) {
	conn.WriteError(fmt.Sprintf("ERR unknown command %s", cmd))
}

func (mkv *MuKV) handlePing(conn redcon.Conn) {
	conn.WriteString("PONG")
}

func (mkv *MuKV) handleQuit(conn redcon.Conn) error {
	conn.WriteString("OK")
	return conn.Close()
}

func (mkv *MuKV) handleSet(conn redcon.Conn, cmd redcon.Command) {
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
			case "px":
				rec, err = mkv.Receive(string(cmd.Args[1]), string(cmd.Args[4]), "ms")
			default:
				rec, err = mkv.Receive(string(cmd.Args[1]), string(cmd.Args[4]), "s")
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
}

func (mkv *MuKV) handleGet(conn redcon.Conn, cmd redcon.Command) {
	logger := mkv.Log.With().Str("function", "handleGet").Logger()
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
}

func (mkv *MuKV) handleDel(conn redcon.Conn, cmd redcon.Command) {
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

func (mkv *MuKV) handleTouch(conn redcon.Conn, cmd redcon.Command) {
	logger := mkv.Log.With().Str("function", "handleTouch").Logger()
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
}

func (mkv *MuKV) handleTTL(conn redcon.Conn, cmd redcon.Command) {
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
}
