package handlers

import (
	"fmt"
	"sync"

	"github.com/Kurler3/go_redis/resp"
)

////////////////////////////////////////////////////////
// DEFINE DATA SETS ////////////////////////////////////
////////////////////////////////////////////////////////

// Normal map
var SETs = map[string]string{}
// Normal map MUTEX
var SETsMu = sync.RWMutex{}

// HSets (map of maps of strings)
var HSets = map[string]map[string]string{}
// HSets MUTEX
var HSetsMu = sync.RWMutex{}

////////////////////////////////////////////////////////
// HANDLERS ////////////////////////////////////////////
////////////////////////////////////////////////////////

// Export handlers map (command to function that takes arguments and returns a value)
var Handlers = map[string]func([]resp.Value) resp.Value {
	"PING": ping,
	"SET": set,
	"GET": get,
	"HSET": hSet,
	"HGET": hGet,
	"HGETALL": hGetAll,
}

// Find
func ping(args []resp.Value) resp.Value {

	if len(args) == 0 {
		return resp.Value {
			Typ: "string",
			Str: "PONG",
		} 
	}

	return resp.Value{
		Typ: "string",
		Str: args[0].Bulk,
	}
}


// Get
func get(args []resp.Value) resp.Value {

	// If args = 0 
	if len(args) == 0 {
		return resp.Value{
			Typ: "error",
			Str: "ERR: No key provided. Example get command: 'GET <key>'",
		}
	}

	// If more than 1 arg => error (too many keys)
	if len(args) > 1 {
		return resp.Value{
			Typ: "error",
			Str: "ERR: Too many keys provided. Example get command: 'GET <key>'",
		}
	}

	// Get key
	key := args[0].Bulk

	// Lock the go routine
	SETsMu.Lock()
    defer SETsMu.Unlock()

	// Get value in map
	value, ok := SETs[key]

	// If key not found
	if !ok {	
		return resp.Value{
			Typ: "error",
			Str: "ERR: Key not found",
		}
	}

	// Return value
	return resp.Value{
		Typ: "bulk",
		Bulk: value,
	}
}

// Set
func set(args []resp.Value) resp.Value {

	// If args smaller than 2
	if len(args) < 2 {
		return resp.Value{
			Typ: "error",
			Str: "ERR: Key and value needed. Example set command: 'SET <key> <value>'",
		}
	}

	// If more than 2 arg => error (too many keys)
	if len(args) > 2 {
		return resp.Value{
			Typ: "error",
			Str: "ERR: Too many keys provided. Example set command: 'SET <key> <value>'",
		}
	}

	// Set the key on the map
	key := args[0].Bulk
	value := args[1].Bulk

	// Lock the go routine
	SETsMu.Lock()
    defer SETsMu.Unlock()

	// Set the key
	SETs[key] = value
	
	// Return
	return resp.Value{
		Typ: "string",
		Str: "OK",
	}
}

// HGet
func hGet(args []resp.Value) resp.Value {

	// If args = 0 
	if len(args) < 2 {
		return resp.Value{
			Typ: "error",
			Str: "ERR: No key provided. Example get command: 'HGET <key> <field>'",
		}
	}

	// If more than 2 arg => error (too many keys)
	if len(args) > 2 {
		return resp.Value{
			Typ: "error",
			Str: "ERR: Too many keys provided. Example get command: 'HGET <key> <field>'",
		}
	}

	// Get key
	key := args[0].Bulk
	field := args[1].Bulk

	// Lock the go routine
	HSetsMu.Lock()
    defer HSetsMu.Unlock()


	// Get hset
	hset, ok := HSets[key]

	if !ok {
		return resp.Value{
			Typ: "error",
			Str: fmt.Sprintf("ERR: HSET with key %s not found", key),
		}
	}

	// Get value in map
	value, ok := hset[field]

	if !ok {
		return resp.Value{
			Typ: "error",
			Str: fmt.Sprintf("ERR: Field with key %s not found on hash set with key %s", field, key),
		}
	}

	// Return value
	return resp.Value{
		Typ: "bulk",
		Bulk: value,
	}

}


// HSet
func hSet(args []resp.Value) resp.Value {

	// If less than 3 args
	if len(args) < 3 {

		return resp.Value{
			Typ: "error",
			Str: "ERR: Missing arguments. Example hset command: HSET <key> <field> <value>",
		}

	}

	// If more than 3 args
	if len(args) > 3 {

		return resp.Value{
			Typ: "error",
			Str: "ERR: Too many arguments. Example hset command: HSET <key> <field> <value>",
		}

	}

	// Get the key
	key := args[0].Bulk

	// Get the field
	field := args[1].Bulk

	// Lock go routine
	HSetsMu.Lock();
	// Defer unlock go routine
	defer HSetsMu.Unlock();

	// Get the value
	value := args[2].Bulk

	// If no hset with this key => init
	if _, ok := HSets[key]; !ok {
		HSets[key] = map[string]string{}
	}

	// Set the value
	HSets[key][field] = value

	// Return OK
	return resp.Value{
		Typ: "string",
		Str: "OK",
	}
} 

// Get all the keys and values of an hashed set
func hGetAll(args []resp.Value) resp.Value {

	// If less than 1 arg
	if len(args) == 0 {
		return resp.Value {
			Typ: "error",
			Str: "ERR: No key provided. Example get command: 'HGETALL <key>'",
		}
	}

	// If more than 1 argument
	if len(args) > 1 {
		return resp.Value {
			Typ: "error",
			Str: "ERR: Too many arguments. Example get command: 'HGETALL <key>'",
		}
	} 

	// Get the key
	key := args[0].Bulk

	// Get the hset	
	hset, ok := HSets[key]

	if !ok {
		return resp.Value{
			Typ: "error",
			Str: fmt.Sprintf("ERR: hash set with key %s not found", key),
		}
	}

	// Init slice 
	results := make([]resp.Value, 0, len(hset))

	// For each key and value in the set, append to the results
	for field, value := range hset {
		results = append(results, resp.Value{
			Typ: "bulk",
			Bulk: field,
		})
		results = append(results, resp.Value{
			Typ: "bulk",
			Bulk: value,
		})
	}

	// Return results
	return resp.Value{
		Typ: "array",
		Array: results,
	}
}

/////////////////////////////////////////////////////
// GET ALL POSSIBLE COMMANDS ////////////////////////
/////////////////////////////////////////////////////

func GetHandlerKeys() []string {
	keys := make([]string, 0, len(Handlers))
	for key := range Handlers {
		keys = append(keys, key)
	}
	return keys
}
