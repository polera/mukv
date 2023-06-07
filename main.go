package main

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/redcon"
)

var mu sync.RWMutex
var datastore sync.Map
var expirationQueue = make(chan record)
var records = make(map[string]*record)

type record struct {
	key     string
	created time.Time
	ttl     time.Duration
	hits    int
}

func (r *record) age() time.Duration {
	return time.Now().Sub(r.created)
}

func (r *record) expired() bool {
	return r.age() >= r.ttl
}

func (r *record) timeToExpiry() float64 {
	if r.ttl == 0 {
		return 0
	}
	return (r.ttl - r.age()).Seconds()
}

func (r *record) touch() {
	r.hits = 0
}

func receive(key string, ttl, duration string) (*record, error) {
	var err error
	var recTTL time.Duration
	if len(ttl) > 0 {
		ttlStr := fmt.Sprintf("%s%s", ttl, duration)
		recTTL, err = time.ParseDuration(ttlStr)

		if err != nil {
			return nil, err
		}

	}

	r := &record{
		key:     key,
		created: time.Now(),
		ttl:     recTTL,
	}

	if r.ttl > 0 {
		go func() {
			expirationQueue <- *r
		}()
	}

	return r, nil
}

func startExpireLoop() {
	for {
		select {
		case rec := <-expirationQueue:
			if rec.timeToExpiry() < .1 {
				fmt.Println("got key: ", rec.key)
				mu.RLock()
				record, ok := records[rec.key]
				mu.RUnlock()
				if !ok {
					log.Println("No record found for key: ", rec.key)
					continue
				}
				if record.ttl == 0 {
					log.Println("Not expiring key, overwritten with no TTL: ", rec.key)
					continue
				}

				log.Println("Expiring key: ", rec.key)
				datastore.Delete(rec.key)
				mu.Lock()
				delete(records, record.key)
				mu.Unlock()
				continue

			}
			go func() {
				expirationQueue <- rec
			}()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func startServer(port int) {
	listenAddr := fmt.Sprintf(":%d", port)

	go startExpireLoop()

	go log.Printf("listening at %s", listenAddr)
	err := redcon.ListenAndServe(listenAddr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError(fmt.Sprintf("ERR unknown command %s", cmd.Args[0]))
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "set":
				argLen := len(cmd.Args)
				if argLen < 3 {
					errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
					conn.WriteError(errStr)
				} else {
					var err error
					var rec *record
					switch argLen {
					case 3:
						rec, err = receive(string(cmd.Args[1]), "", "")
					case 5:
						switch strings.ToLower(string(cmd.Args[3])) {
						default:
							rec, err = receive(string(cmd.Args[1]), string(cmd.Args[4]), "s")
						case "px":
							rec, err = receive(string(cmd.Args[1]), string(cmd.Args[4]), "ms")
						}
					}
					if err != nil {
						conn.WriteError(err.Error())
					} else {
						datastore.Store(rec.key, cmd.Args[2])
						mu.Lock()
						records[rec.key] = rec
						mu.Unlock()
						conn.WriteString("OK")
					}

				}
			case "get":
				if len(cmd.Args) != 2 {
					errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
					conn.WriteError(errStr)
				} else {
					rec, ok := datastore.Load(string(cmd.Args[1]))
					if !ok {
						conn.WriteNull()
					} else {
						mu.Lock()
						r := records[string(cmd.Args[1])]
						r.hits += 1
						mu.Unlock()
						log.Printf("Hits for %s: %d", r.key, r.hits)
						conn.WriteAny(rec)
					}
				}
			case "ttl":
				if len(cmd.Args) != 2 {
					errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
					conn.WriteError(errStr)
				} else {
					_, ok := datastore.Load(string(cmd.Args[1]))
					if !ok {
						conn.WriteNull()
					} else {
						mu.RLock()
						r := records[string(cmd.Args[1])]
						mu.RUnlock()
						conn.WriteInt(int(r.timeToExpiry()))
					}
				}
			case "del":
				if len(cmd.Args) != 2 {
					errStr := fmt.Sprintf("ERR wrong number of arguments for %s", cmd.Args[0])
					conn.WriteError(errStr)
				} else {
					datastore.Delete(string(cmd.Args[1]))
					mu.Lock()
					_, ok := records[string(cmd.Args[1])]
					delete(records, string(cmd.Args[1]))
					mu.Unlock()
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
		log.Fatal(err)
	}
}

func main() {
	startServer(6480)
}
