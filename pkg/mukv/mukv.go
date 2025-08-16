package mukv

import (
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type MuKV struct {
	sync.RWMutex
	Log             zerolog.Logger
	Datastore       sync.Map
	expirationQueue chan Record
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
			mkv.expirationQueue <- *r
		}()
	}

	return r, nil
}

func (mkv *MuKV) StartExpireLoop() {
	logger := mkv.Log.With().Str("function", "StartExpireLoop").Logger()
	for {
		// Delay 10 milliseconds between expiration queue sweeps
		delayTimer := time.NewTimer(10 * time.Millisecond)
		rec := <-mkv.expirationQueue
		if rec.TimeToExpiry() < .1 {
			mkv.RWMutex.RLock()
			record, ok := mkv.Records[rec.Key]
			mkv.RWMutex.RUnlock()
			if !ok {
				logger.Error().Str("key", rec.Key).Msg("no record found")
				continue
			}
			if record.TTL == 0 {
				logger.Warn().Str("key", rec.Key).Msg("not expiring key, overwritten with no TTL")
				continue
			}

			logger.Debug().Str("key", rec.Key).Msg("expiring key")
			mkv.Datastore.Delete(rec.Key)
			mkv.RWMutex.Lock()
			delete(mkv.Records, record.Key)
			mkv.RWMutex.Unlock()
			continue

		}
		go func() {
			mkv.expirationQueue <- rec
		}()

		<-delayTimer.C
	}
}

func New(logger zerolog.Logger) *MuKV {
	expirationQueue := make(chan Record)
	records := make(map[string]*Record)

	return &MuKV{
		RWMutex:         sync.RWMutex{},
		Datastore:       sync.Map{},
		expirationQueue: expirationQueue,
		Records:         records,
		Log:             logger,
	}
}

type Record struct {
	Key     string
	Created time.Time
	TTL     time.Duration
	Hits    int
}

func (r *Record) Age() time.Duration {
	return time.Since(r.Created)
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
