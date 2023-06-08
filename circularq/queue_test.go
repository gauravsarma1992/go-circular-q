package circularq

import (
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func DummyQueue() (q *CircularQ) {
	q, _ = New(context.TODO(), LogFlusherFunc)
	return
}

func FillBuffer(q *CircularQ, n int) {
	for i := 0; i < n; i++ {
		q.Store(i)
	}
}

func LogFlusherFunc(messages []Message) (err error) {
	if len(messages) == 0 {
		return
	}
	log.Println("Start Value - ", messages[0].(int))
	log.Println("Stop Value - ", messages[len(messages)-1].(int))
	return
}

func TestNew(t *testing.T) {
	q, _ := New(context.TODO(), LogFlusherFunc)

	assert.Equal(t, q.config.FrequencyThreshold, 1000)
	assert.Equal(t, q.config.RolloverThreshold, 10000)
	assert.Equal(t, q.config.TimeThresholdInSecs, 5)
}

func TestStoreWhenNotFull(t *testing.T) {
	q := DummyQueue()
	FillBuffer(q, 100)

	err := q.Store(1)
	assert.Nil(t, err)
	log.Println(q.Length(), q.startIdx, q.stopIdx)
	assert.Equal(t, q.Length(), 101)
}

func TestStoreWhenFull(t *testing.T) {
	q := DummyQueue()
	FillBuffer(q, q.config.RolloverThreshold)

	err := q.Store(1)
	assert.NotNil(t, err)
	assert.Equal(t, q.Length(), q.config.RolloverThreshold)
}

func TestStoreWhenFlushedAfterBeingFull(t *testing.T) {
	q := DummyQueue()
	FillBuffer(q, q.config.RolloverThreshold)

	q.Flush()
	log.Println(q.Length(), q.startIdx, q.stopIdx)
	err := q.Store(100)
	log.Println(q.Length(), q.startIdx, q.stopIdx)
	assert.Nil(t, err)
	assert.Equal(t, q.Length(), q.config.RolloverThreshold-q.config.FrequencyThreshold)
}

func TestFlushWhenEmpty(t *testing.T) {
	q := DummyQueue()
	err := q.Flush()
	assert.Nil(t, err)
	assert.Equal(t, q.Length(), 0)
}

func TestFlushWhenLengthLesserThanFrequencyThreshold(t *testing.T) {
	q := DummyQueue()
	FillBuffer(q, 100)

	assert.Equal(t, q.Length(), 100)
	err := q.Flush()
	assert.Nil(t, err)
	assert.Equal(t, q.Length(), 0)
}

func TestFlushWhenLengthGreaterThanFrequencyThresholdByOne(t *testing.T) {
	q := DummyQueue()
	FillBuffer(q, q.config.FrequencyThreshold+1)

	assert.Equal(t, q.Length(), q.config.FrequencyThreshold+1)
	err := q.Flush()
	assert.Nil(t, err)
	assert.Equal(t, q.Length(), 1)
}

func TestFlushWhenLengthGreaterThanFrequencyThresholdByHundred(t *testing.T) {
	q := DummyQueue()
	FillBuffer(q, q.config.FrequencyThreshold+100)

	assert.Equal(t, q.Length(), q.config.FrequencyThreshold+100)
	err := q.Flush()
	assert.Nil(t, err)
	assert.Equal(t, q.Length(), 100)
}
