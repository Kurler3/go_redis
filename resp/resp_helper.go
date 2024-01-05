package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Types of command
const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

// Value struct.
type Value struct {
	typ   string // Type of command
	str   string // Value of the string received
	num   int // Value of the num received (if type is INTEGER)
	bulk  string // Value of the bulk string received (if type is BULK)
	array []Value // Value of the array of values if the type is ARRAY
}

// RESP struct (has a BUFIO reader)
type Resp struct {
	reader *bufio.Reader
}

// Create a new RESP object
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}


// Read (function that will read the entire buffer recursively)
func (r *Resp) Read() (Value, error) {

	// Read the type
	_type, err := r.reader.ReadByte()

	// If there was an error reading the type
	if err != nil {
		return Value{}, err
	}

	// Switch on the type
	switch _type {
		// If array
		case ARRAY:
			return r.readArray()
		// If bulk
		case BULK:
			return r.readBulk()
		// Default (unknown type)
		default:
			fmt.Printf("Unknown type: %v\n", string(_type))
			return Value{}, nil
	}
}

// readLine reads the line from the buffer.
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {

		// Get the next byte in the buffer
		b, err := r.reader.ReadByte()
		
		// If error is not null
		if err != nil {
			return nil, 0, err
		}

		// Increment the number of chars
		n += 1

		// Append the byte to the line
		line = append(line, b)

		// If the current line has more than 2 chars and the second to last char is a carriage return (new line) => break the loop
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], n, nil
}

// readInteger reads the integer from the buffer.
func (r *Resp) readInteger() (int, int, error) {

	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}


// *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n

// Read array
func (r *Resp) readArray() (Value, error) {

	// Init value
	v := Value{
		typ: "array",
	}

	// Get the size of the array
	len, _, err := r.readInteger()

	if err != nil {
		return Value{}, err
	}	

	// foreach line, parse and read the value
	v.array = make([]Value, 0)

	// For each item in the array
	for i := 0; i < len; i++ {

		// Read the value
		val, err := r.Read()

		if err != nil {
			return Value{}, err
		}

		// Append the value to the array
		v.array = append(v.array, val)
	}

	return v, nil

}

// Read bulk
func (r *Resp) readBulk() (Value, error) {

	v := Value{
		typ: "bulk",
	}

	// Read size
	size, _, err := r.readInteger()

	if err != nil {
		return Value{}, err
	}

	// Make buffer of size
	buff := make([]byte, size)

	// Read the line
	_, err = r.reader.Read(buff)

	if err != nil {
		return Value{}, err
	}

	// Convert to string and set on the v.bulk
	v.bulk = string(buff)

	// Read the trailing CRLF
	r.readLine()

	// Return the value
	return v, nil
}
