package main

import (
	"fmt"
	"net"
	"os"

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

	// Accept connections
	conn, err := l.Accept()

	if err != nil {
		fmt.Println("Error while accepting connection: ", err)
		os.Exit(1)
	}

	fmt.Println("Connection accepted!")

	// Disconnect before returning
	defer conn.Close()

	// Receive commands from clients
	for {

		newResp := resp.NewResp(conn)
		value, err := newResp.Read()

		if err != nil {
			fmt.Println(err)
			break
		}

		fmt.Println(value)

		// ignore request and send back a PONG
		conn.Write([]byte("+OK\r\n"))
	}

	// Close connection
	fmt.Println("Connection closed! See ya next time :D");
}
