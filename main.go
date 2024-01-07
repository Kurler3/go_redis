package main

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/Kurler3/go_redis/aof"
	"github.com/Kurler3/go_redis/handlers"
	"github.com/Kurler3/go_redis/resp"
)

func main() {

	// Listen with TCP on port 8080
	l, err := net.Listen("tcp", ":6379")

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Listening on port 6379")

	// Open (create also if doesn't exist) AOF File
	aof, err := aof.NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}

	// Close AOF file at the end of the program
	defer aof.Close()

	// Read the AOF file
	aof.Read(func(value resp.Value) {

		// Get command
		command := strings.ToUpper(value.Array[0].Bulk)
		
		// Get the arguments
		args := value.Array[1:]

		// Get handler and check if it is ok or not (if not ok => log error)
		handler, ok := handlers.Handlers[command]

		// If not ok
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		// Write to in memory sets
		handler(args)
	})

	// Accept connections
	conn, err := l.Accept()

	// If no error while connecting to client
	if err != nil {
		fmt.Println("Error while accepting connection: ", err)
		os.Exit(1)
	}

	// Log connection accepted
	fmt.Println("Connection accepted!")

	// Disconnect before returning
	defer conn.Close()

	// Receive commands from clients
	for {

		// Get the RESP object
		newResp := resp.NewResp(conn)

		// Get the value from the RESP object as a golang struct 
		value, err := newResp.Read()

		if err != nil {
			fmt.Println(err)
			break
		}

		// Check if the type is array (needs to be)
		if value.Typ != "array" {
			fmt.Println("Invalid request type. Expecting array")
			break
		}

		// Check if more than 0 items in the array
		if len(value.Array) == 0 {
			fmt.Println("Invalid request. Expecting at least 1 item")
			break
		}

		// Write back to client
		writer := resp.NewWriter(conn)


		// Get the command (first item of the array.bulk)
		command := strings.ToUpper(value.Array[0].Bulk)

		if command == "COMMAND" {
			// Write the result to the client
			writer.Write(resp.Value{Typ: "string", Str: "CONNECTED"})
			continue
		}

		// Get the arguments (everything but the first value on the array)
		args := value.Array[1:]

		// Find the handler and check if it is ok or not (if not ok => log error)
		handler, ok := handlers.Handlers[command]

		// If didn't find handler for specified command, show error and available commands.
		if !ok {
			keys := handlers.GetHandlerKeys()
			fmt.Println("Invalid command. Expecting: ", keys)
			writer.Write(resp.Value{Typ: "string", Str: "Invalid command",})
			continue
		}

		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}

		// Get the result from the handler (array of bytes)
		result := handler(args)

		// Write the result to the client
		writer.Write(result)

	}

	// Close connection
	fmt.Println("Connection closed! See ya next time :D");
}
