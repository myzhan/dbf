package dbf

import (
	"fmt"
	"math/rand"
	"time"

	"sync"
	"sync/atomic"

	gouuid "github.com/satori/go.uuid"
)

func doNothing(params map[string]interface{}) interface{} {
	return params["value"]
}

func uuid(params map[string]interface{}) interface{} {
	u := gouuid.NewV4()
	return u.String()
}

func timestamp(params map[string]interface{}) interface{} {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func date(params map[string]interface{}) interface{} {
	currentTime := time.Now().Local()
	return currentTime.Format("2006-01-02")
}

func randomInt64(params map[string]interface{}) interface{} {
	min, ok := params["min"]
	if !ok {
		min = float64(0)
	}
	max, ok := params["max"]
	if !ok {
		max = float64(100)
	}
	result := int64(min.(float64)) + rand.Int63n(int64(max.(float64)))
	return result
}

var letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomString(params map[string]interface{}) interface{} {

	alphabet, ok := params["alphabet"]
	if !ok {
		alphabet = letters
	}

	length, ok := params["length"]
	if !ok {
		length = float64(10)
	}

	slice := make([]byte, int(length.(float64)))

	for i := range slice {
		slice[i] = alphabet.(string)[rand.Intn(len(alphabet.(string)))]
	}

	return string(slice)
}

func randomChoice(params map[string]interface{}) interface{} {

	values, ok := params["value"]
	if !ok {
		panic("randomChoice: must have value")
	}

	randomValue := values.([]interface{})[rand.Intn(len(values.([]interface{})))]

	return randomValue
}

var sequenceStart = int64(0)
var sequenceLock = sync.Mutex{}
var sequenceInit = false

func sequence(params map[string]interface{}) interface{} {

	offset, ok := params["start"]
	if ok && !sequenceInit {
		sequenceLock.Lock()
		atomic.StoreInt64(&sequenceStart, int64(offset.(float64)))
		sequenceInit = true
		sequenceLock.Unlock()
	}

	if !sequenceInit {
		sequenceLock.Lock()
		sequenceInit = true
		sequenceLock.Unlock()
	}

	return atomic.AddInt64(&sequenceStart, 1)
}

func resolveMutatorAndExec(name string, params map[string]interface{}) interface{} {
	mutator, ok := mutators[name]
	if !ok {
		return fmt.Sprintf("%s, undefined", name)
	}
	return mutator(params)
}

var mutators = make(map[string]func(params map[string]interface{}) interface{})

func init() {

	// initialize global pseudo random generator
	rand.Seed(time.Now().Unix())

	mutators["const"] = doNothing
	mutators["uuid"] = uuid
	mutators["timestamp"] = timestamp
	mutators["date"] = date
	mutators["randomInt"] = randomInt64
	mutators["randomString"] = randomString
	mutators["randomChoice"] = randomChoice
	mutators["sequence"] = sequence
}
