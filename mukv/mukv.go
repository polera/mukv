package mukv

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type MuKV struct {
	sync.RWMutex
	Datastore       sync.Map
	ExpirationQueue chan Record
	Records         map[string]*Record
}

func (mkv *MuKV) Receive(key string, ttl, duration string) (*Record, error) {
	var err error
	var recTTL time.Duration
	if len(ttl) > 0 {
		ttlStr := fmt.Sprintf("%s%s", ttl, duration)
		recTTL, err = time.ParseDuration(ttlStr)

		if err != nil {
			return nil, err
		}

	}

	r := &Record{
		Key:     key,
		Created: time.Now(),
		TTL:     recTTL,
	}

	if r.TTL > 0 {
		go func() {
			mkv.ExpirationQueue <- *r
		}()
	}

	return r, nil
}

func (mkv *MuKV) StartExpireLoop() {
	for {
		select {
		case rec := <-mkv.ExpirationQueue:
			if rec.TimeToExpiry() < .1 {
				fmt.Println("got Key: ", rec.Key)
				mkv.RWMutex.RLock()
				record, ok := mkv.Records[rec.Key]
				mkv.RWMutex.RUnlock()
				if !ok {
					log.Println("No Record found for Key: ", rec.Key)
					continue
				}
				if record.TTL == 0 {
					log.Println("Not expiring Key, overwritten with no TTL: ", rec.Key)
					continue
				}

				log.Println("Expiring Key: ", rec.Key)
				mkv.Datastore.Delete(rec.Key)
				mkv.RWMutex.Lock()
				delete(mkv.Records, record.Key)
				mkv.RWMutex.Unlock()
				continue

			}
			go func() {
				mkv.ExpirationQueue <- rec
			}()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func New() *MuKV {
	expirationQueue := make(chan Record)
	records := make(map[string]*Record)

	return &MuKV{
		RWMutex:         sync.RWMutex{},
		Datastore:       sync.Map{},
		ExpirationQueue: expirationQueue,
		Records:         records,
	}
}

type Record struct {
	Key     string
	Created time.Time
	TTL     time.Duration
	Hits    int
}

func (r *Record) Age() time.Duration {
	return time.Now().Sub(r.Created)
}

func (r *Record) Expired() bool {
	return r.Age() >= r.TTL
}

func (r *Record) TimeToExpiry() float64 {
	if r.TTL == 0 {
		return 0
	}
	return (r.TTL - r.Age()).Seconds()
}

func (r *Record) Touch() {
	r.Hits = 0
}
