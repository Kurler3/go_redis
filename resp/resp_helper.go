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
	Typ   string // Type of command
	Str   string // Value of the string received
	Num   int // Value of the num received (if type is INTEGER)
	Bulk  string // Value of the bulk string received (if type is BULK)
	Array []Value // Value of the array of values if the type is ARRAY
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

// Read array
func (r *Resp) readArray() (Value, error) {

	// Init value
	v := Value{
		Typ: "array",
	}

	// Get the size of the array
	len, _, err := r.readInteger()

	if err != nil {
		return Value{}, err
	}	

	// foreach line, parse and read the value
	v.Array = make([]Value, 0)

	// For each item in the array
	for i := 0; i < len; i++ {

		// Read the value
		val, err := r.Read()

		if err != nil {
			return Value{}, err
		}

		// Append the value to the array
		v.Array = append(v.Array, val)
	}

	return v, nil

}

// Read bulk
func (r *Resp) readBulk() (Value, error) {

	v := Value{
		Typ: "bulk",
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
	v.Bulk = string(buff)

	// Read the trailing CRLF
	r.readLine()

	// Return the value
	return v, nil
}


// Marshal Value to byte array (to RESP response)
func (v Value) Marshal() []byte {
	switch v.Typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshallNull()
	case "error":
		return v.marshallError()
	default:
		return []byte{}
	}
}


// marshal String
func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

// marshal Array
func (v Value) marshalArray() []byte {

	// Get the len
	len := len(v.Array)

	// Init bytes array
	var bytes []byte

	// Append array type
	bytes = append(bytes, ARRAY)

	// Append array size
	bytes = append(bytes, strconv.Itoa(len)...)

	// Append CRLF
	bytes = append(bytes, '\r', '\n')

	// For each item in the array => append their bytes
	for _, item := range v.Array {
		bytes = append(bytes, item.Marshal()...)
	}

	return bytes
}

// marshal Bulk
func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.Bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.Bulk...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

// marshal Error
func (v Value) marshallError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// marshal Null
func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}



// Writer struct (will write back to the client)
type Writer struct {
	writer io.Writer
}

// Get a new writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

// Write to the client
func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}