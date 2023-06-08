package circularq

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"time"
)

var (
	BaseConfigFolder  = os.Getenv("CONFIG_FOLDER")
	DefaultConfigFile = BaseConfigFolder + "/config.json"
)

type (
	Message     interface{}
	FlusherFunc func([]Message) error

	CircularQ struct {
		Ctx    context.Context
		store  []Message
		config *Config

		// The startIdx and stopIdx represent the starting and ending indices of
		// the store. The store is a circular buffer, so the startIdx and stopIdx
		// are not necessarily the first and last indices of the store.
		// The stopIdx is incremented by 1 after every Store() call.
		// The startIdx is set to the length of the flushed messages after every
		// Flush() call.
		// In cases where the startIdx is greater than the stopIdx, the length
		// of the store is calculated as (stopIdx - startIdx).
		// When the startIdx is less than the stopIdx, the length of the store
		// is calculated as (stopIdx - startIdx).
		// When the startIdx or stopIdx exceeds the RollOverThreshold, it is reset to 0.
		// The stopIdx is not included in the calculation of the length of the store.
		// So if the startIdx is 0 and stopIdx is 100, the length of the store is 100.
		// If the stopIdx is 100, it means we can flush messages till the 99th index.
		// The startIdx is inclusive of the value as well.
		// After a flush happens, the startIdx is set to the length of the flushed messages.
		// For example, if the startIdx is at 10 and the length of flushed events is 90,
		// then the startIdx should be set to 90 + 10 = 100, which means the next value is
		// at index 100. The stopIdx should be set to startIdx + 1
		startIdx      int
		stopIdx       int
		lastFlushTime time.Time

		FlusherFunc FlusherFunc
	}

	Config struct {
		FrequencyThreshold  int `json:"frequency_threshold"`
		RolloverThreshold   int `json:"rollover_threshold"`
		TimeThresholdInSecs int `json:"time_threshold_in_secs"`
	}
)

func New(ctx context.Context, flusherFunc FlusherFunc) (q *CircularQ, err error) {
	q = &CircularQ{
		Ctx:         ctx,
		FlusherFunc: flusherFunc,
	}
	if q.config, err = q.readConfig(); err != nil {
		return
	}
	return
}

func (q *CircularQ) readConfig() (config *Config, err error) {
	var (
		confB []byte
	)
	config = &Config{}
	if confB, err = ioutil.ReadFile(DefaultConfigFile); err != nil {
		return
	}
	if err = json.Unmarshal(confB, &config); err != nil {
		return
	}
	return
}

func (q *CircularQ) Store(msg Message) (err error) {
	if q.IsFull() {
		err = errors.New("Queue is full")
		return
	}
	q.store = append(q.store, msg)
	if err = q.PostStore(); err != nil {
		return
	}
	return
}

func (q *CircularQ) PostStore() (err error) {
	q.stopIdx = q.IncrIdx(q.stopIdx, 1)
	return
}

func (q *CircularQ) IncrIdx(val int, count int) (incrIdx int) {
	for idx := 0; idx < count; idx++ {
		incrIdx = val + 1
		if incrIdx > q.config.RolloverThreshold {
			incrIdx = 0
		}
	}
	return
}

func (q *CircularQ) IsFull() (isFull bool) {
	if q.Length() >= q.config.RolloverThreshold {
		isFull = true
		return
	}
	return
}

func (q *CircularQ) IsEmpty() (isEmpty bool) {
	if q.Length() == 0 {
		isEmpty = true
		return
	}
	return
}

func (q *CircularQ) Length() (length int) {
	if q.stopIdx >= q.startIdx {
		length = q.stopIdx - q.startIdx
		return
	}
	if q.stopIdx < q.startIdx {
		length = q.startIdx - q.stopIdx
	}
	return
}

func (q *CircularQ) HasFrequencyThresholdPassed() (hasPassed bool) {
	if q.Length() >= q.config.FrequencyThreshold {
		hasPassed = true
		return
	}
	return
}

func (q *CircularQ) HasTimeThresholdPassed() (hasPassed bool) {
	if time.Since(q.lastFlushTime) > time.Duration(q.config.TimeThresholdInSecs)*time.Second {
		hasPassed = true
		return
	}
	return
}

func (q *CircularQ) ShouldFlush() (shouldFlush bool) {
	if q.HasTimeThresholdPassed() {
		shouldFlush = true
		return
	}
	if q.HasFrequencyThresholdPassed() {
		shouldFlush = true
		return
	}
	return
}

func (q *CircularQ) GetMessages() (messages []Message, err error) {
	batchStopSize := q.Length()
	if batchStopSize > q.config.FrequencyThreshold {
		batchStopSize = q.config.FrequencyThreshold
	}
	for idx := q.startIdx; idx < batchStopSize; idx++ {
		messages = append(messages, q.store[idx])
	}
	return
}

func (q *CircularQ) Flush() (err error) {

	var (
		messages []Message
	)

	if messages, err = q.GetMessages(); err != nil {
		return
	}

	if err = q.FlusherFunc(messages); err != nil {
		return
	}

	if err = q.PostFlush(len(messages)); err != nil {
		return
	}
	return
}

func (q *CircularQ) PostFlush(eventLen int) (err error) {
	q.startIdx = q.IncrIdx(q.startIdx, eventLen)
	q.stopIdx = q.IncrIdx(q.startIdx, 1)
	q.lastFlushTime = time.Now()
	return
}

func (q *CircularQ) FlushAll() (err error) {
	for {
		if q.IsEmpty() {
			break
		}
		if err = q.Flush(); err != nil {
			return
		}
	}
	return
}
