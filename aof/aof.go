package aof

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Kurler3/go_redis/resp"
)

// Define AOF struct
type Aof struct {
	file *os.File // Pointer to the AOF File
	rd   *bufio.Reader // Reader for the AOF File
	mu   sync.Mutex // Mutex for the AOF File
}

// New AOF function
func NewAof(path string) (*Aof, error) {

	// Create a new file if it doesn't exist
	f, err := os.OpenFile(path, os.O_CREATE | os.O_RDWR, 0666)
	if err != nil {
		// Return the error
		return nil, err
	}

	// Init a pointer to a AOF instance
	aof := &Aof{
		file: f,
		rd:   bufio.NewReader(f),
		mu:   sync.Mutex{},
	}

	// Start a goroutine to sync AOF to disk every 1 second
	go func() {
		for {

			// Lock the go routine
			aof.mu.Lock()

			// Sync the AOF file pointer to DISK (updates the file in the system every 1 second)
			aof.file.Sync()

			// Unlock the go routine
			aof.mu.Unlock()

			// Wait for a second
			time.Sleep(time.Second)

		}
	}()

	// Return the AOF instance
	return aof, nil
}

// Close file (when the server shuts down)
func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Close()
}

// Write value to file
func (aof *Aof) Write(value resp.Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	// Right to file
	_, err := aof.file.Write(value.Marshal())
	if err != nil {
		return err
	}
	return nil
}

// Read AOF file 
func (aof *Aof) Read(writeFunc func(value resp.Value)) {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	
	// Read from file
	// Get the RESP object
	newResp := resp.NewResp(aof.file)

	// Init values slice
	values := make([]resp.Value, 0)

	for {

		// Get the value from the RESP object as a golang struct 
		value, err := newResp.Read()

		if err != nil {

			if err == io.EOF { break }

			fmt.Println("ERR: Error reading from AOF File: ", err.Error())

			os.Exit(1)
		}

		// Append the value
		values = append(values, value)
	}
	
	// For each value in the slice, call the readFunc function
	for _, value := range values {
		writeFunc(value)
	}
	
}
