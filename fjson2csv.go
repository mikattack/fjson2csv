package fjson2csv

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

/*
 * The following assumptions are made when converting JSON input:
 *
 *  - Input JSON is a single collection (array) of objects
 *  - Each object contains only properties with scalar values
 *    (no nested objects)
 *  - No expected consistency of property names from object to object
 *    (eg. no fixed schema)
 *  - No string values of properties contain a CSV delimiter
 *    (a comma, by default)
 *  - CSV headers are always included
 *  - All properties are included in CSV output, even if an object is
 *    missing them
 *  - CSV fields are sorted by their frequency, then alphabetically
 */

const default_delimiter string = ","
const default_write_buffer_size int = 1024
const default_read_buffer_size int = 1024

// Converts JSON into CSV incrementally.
func UnbufferedConvert(r io.ReadSeeker, w io.Writer, opts Options) error {
	rsize, wsize := getBufferSizes(opts)
	c := converter{
		Source:      r,
		Destination: w,
		Keys:        map[string]int64{},
		delimiter:   default_delimiter,
		sorted:      []string{},
		readSize:		 rsize,
		writeSize:	 wsize,
	}
	c.IndexFields(extractKeys)
	c.WriteCsv(writeRecord)
	if c.err != nil {
		return c.err
	}
	return nil
}

// Converts JSON into CSV in-memory.
func BufferedConvert(r io.ReadSeeker, w io.Writer, opts Options) error {
	rsize, wsize := getBufferSizes(opts)
	c := converter{
		Source:      r,
		Destination: w,
		Keys:        map[string]int64{},
		buffer:      []map[string]interface{}{},
		delimiter:   default_delimiter,
		sorted:      []string{},
		readSize:		 rsize,
		writeSize:	 wsize,
	}

	c.IndexFields(bufferData)
	ew := newErrorWriter(c.Destination, c.writeSize)

	// Write field headers
	ew.write(fmt.Sprintf("%s\n", strings.Join(c.sorted, c.delimiter)))

	// Write buffered data
	for i := 0; i < len(c.buffer); i++ {
		record := c.buffer[i]
		if value, ok := record[c.sorted[0]]; ok == true {
			ew.write(value)
		}
		for _, key := range c.sorted[1:] {
			var value interface{} = ""
			if _, ok := record[key]; ok == true {
				value = record[key]
			}
			ew.write(c.delimiter)
			ew.write(value)
		}
		ew.write("\n")
		if ew.err != nil {
			c.err = ew.err
			break
		}
	}
	ew.flush()
	if c.err != nil {
		return c.err
	}

	return nil
}

type Options struct {
	ReadBufferSize	int
	WriteBufferSize	int
}

// Convenience type for cutting down on error checking and type conversion
// boilerplate code during repetative writes.
type errWriter struct {
	w   *bufio.Writer
	err error
}

func (ew *errWriter) write(value interface{}) {
	if ew.err == nil {
		data := []byte(toString(value))

		// Avoid growing the buffer
		if len(data) > ew.w.Available() {
			err := ew.w.Flush()
			if err != nil {
				ew.err = err
				return
			}
		}
		_, ew.err = ew.w.Write(data)
	}
}

func (ew *errWriter) flush() {
	if ew.err == nil {
		ew.err = ew.w.Flush()
	}
}

func newErrorWriter(writer io.Writer, size int) *errWriter {
	return &errWriter {
		w: bufio.NewWriterSize(writer, size),
	}
}

// Prototype for functions used as callbacks during JSON structure walks.
type walkFunction func(record map[string]interface{}, args ...interface{}) error

// Encapsulates the state necessary to convert JSON input to CSV output.
//
// This implementation passes over input data twice to first extract all
// possible field names, then to output CSV data. This trades greater time
// complexity for less space complexity.
type converter struct {
	Source      io.ReadSeeker
	Destination io.Writer
	Keys        map[string]int64
	delimiter   string
	buffer      []map[string]interface{}
	err         error
	readSize		int
	sorted      []string
	writeSize		int
}

// Walks a flat JSON array, invoking the given callback for each object
// encountered. The callback is passed `map[string]interface{}` deserializaiton
// of each object.
func (c *converter) WalkJsonList(fn walkFunction, args ...interface{}) {
	dec := json.NewDecoder(bufio.NewReaderSize(c.Source, c.readSize))

	// Opening bracket
	if token, err := dec.Token(); err != nil {
		c.err = fmt.Errorf("malformed JSON")
		return
	} else {
		delim, ok := token.(json.Delim)
		if ok == false || delim.String() != "[" {
			c.err = fmt.Errorf("malformed JSON: document must be an array of objects")
		}
	}

	// Scan each record and extract key names and frequencies
	for dec.More() {
		var record interface{}
		if err := dec.Decode(&record); err != nil {
			c.err = err
			return
		} else {
			m := record.(map[string]interface{})
			if err := fn(m, args...); err != nil {
				c.err = err
				return
			}
		}
	}

	// Closing bracket
	if _, err := dec.Token(); err != nil {
		c.err = fmt.Errorf("malformed JSON: array does not end properly")
		return
	}

	// Rewind file cursor
	if _, err := c.Source.Seek(0, 0); err != nil {
		c.err = fmt.Errorf("file read failure: %s", err.Error())
		return
	}
}

// Extracts all property names from JSON input.
func (c *converter) IndexFields(fn walkFunction) {
	// Extract keys
	c.WalkJsonList(fn, c)

	// Sort keys by frequency
	c.sorted = make([]string, len(c.Keys))
	i := 0
	for k, _ := range c.Keys {
		c.sorted[i] = k
		i++
	}
	sort.Sort(c)
}

// Writes the CSV version of all data in the JSON input to the
// converter's writer.
func (c *converter) WriteCsv(fn walkFunction) {
	if c.err != nil {
		return
	}
	if len(c.sorted) == 0 {
		return
	}

	w := newErrorWriter(c.Destination, c.writeSize)

	// Write field headers
	w.write(fmt.Sprintf("%s\n", strings.Join(c.sorted, c.delimiter)))

	// Write JSON data as CSV
	c.WalkJsonList(fn, c, w)
	w.flush()
}

// Callback function that indexes record keys.
func extractKeys(record map[string]interface{}, args ...interface{}) error {
	c := args[0].(*converter)
	for key, _ := range record {
		if _, ok := c.Keys[key]; ok == false {
			c.Keys[key] = 0
		}
		c.Keys[key] += 1
	}
	return nil
}

// Callback function that buffers and indexes record keys.
func bufferData(record map[string]interface{}, args ...interface{}) error {
	c := args[0].(*converter)
	c.buffer = append(c.buffer, record)
	return extractKeys(record, args...)
}

// Callback function which outputs record values to a writer according to the
// given key map and delimiter.
func writeRecord(record map[string]interface{}, args ...interface{}) error {
	c := args[0].(*converter)
	w := args[1].(*errWriter)

	// Write first value (for delimiter reasons)
	if value, ok := record[c.sorted[0]]; ok == true {
		w.write(value)
	}

	// Write subsequent values
	for _, key := range c.sorted[1:] {
		var value interface{} = ""
		if _, ok := record[key]; ok == true {
			value = record[key]
		}
		w.write(c.delimiter)
		w.write(value)
	}

	// Finish off line
	w.write("\n")

	return w.err
}

/*
 * Make the keys extracted by converter sortable by frequency/key name.
 */
func (c converter) Len() int      { return len(c.sorted) }
func (c converter) Swap(i, j int) { c.sorted[i], c.sorted[j] = c.sorted[j], c.sorted[i] }
func (c converter) Less(i, j int) bool {
	a, b := c.Keys[c.sorted[i]], c.Keys[c.sorted[j]]
	if a != b {
		return a > b
	} else {
		return c.sorted[j] > c.sorted[i]
	}
}

func getBufferSizes(opts Options) (int, int) {
	rsize := default_read_buffer_size
	wsize := default_write_buffer_size
	if opts.ReadBufferSize < 1 {
		rsize = opts.ReadBufferSize
	}
	if opts.WriteBufferSize < 1 {
		wsize = opts.WriteBufferSize
	}
	return rsize * 1000, wsize * 1000
}

// Converts JSON values into strings.
func toString(value interface{}) string {
	switch value.(type) {
	case string:
		return value.(string)
	case float64:
		return strconv.FormatInt(int64(value.(float64)), 10)
	case bool:
		if value.(bool) {
			return "true"
		} else {
			return "false"
		}
	default:
		return ""
	}
}
