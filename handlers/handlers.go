package handlers

import "github.com/Kurler3/go_redis/resp"

// Export handlers map (command to function that takes arguments and returns a value)
var Handlers = map[string]func([]resp.Value) resp.Value{
	"PING": ping,
}

func ping(args []resp.Value) resp.Value {
	return resp.Value {
		Typ: "string",
		Str: "PONG",
	}
}


func GetHandlerKeys() []string {
	keys := make([]string, 0, len(Handlers))
	for key := range Handlers {
		keys = append(keys, key)
	}
	return keys
}
