package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {

	// Listen with TCP on port 8080
	l, err := net.Listen("tcp", ":8080")

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Accept connections
	conn, err := l.Accept()

	if err != nil {
		fmt.Println("Error while accepting connection: ", err)
		os.Exit(1)
	}

	// Disconnect before returning
	defer conn.Close()

	// Receive commands from clients
	for {

		// Make buffer
		buff := make([]byte, 1024)

		// Put conn data in buffer
		_, err := conn.Read(buff)

		// If error and not EOF (in this case end of connection => break loop)
		if err != nil {

			if err == io.EOF {
				break
			}

			fmt.Println("Error while reading data: ", err)
			os.Exit(1)

		}

		buff.

		// Write back to client
		conn.Write([]byte("PONG\r\n"));
	}


	// Close connection
	fmt.Println("Connection closed! See ya next time");
}
