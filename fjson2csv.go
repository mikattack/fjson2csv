package fjson2csv

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)


const default_delimiter string = ","


// Converts a flat, heterogeneous list of JSON objects into CSV. All data
// from the input reader is converted to CSV and output to the writer.
// 
// The following assumptions are made about the JSON input:
//   - A single collection (array) of objects
//   - Each object contains only properties with scalar values
//   - No expected consistency of property names from object to object
//   - No string values of properties contain a CSV delimiter (default: ",")
// 
// Output CSV data always includes field headers.
func Convert(r io.ReadSeeker, w io.Writer) error {
	c := converter {
		Source: 			r,
		Destination:	w,
		Keys:					map[string]int64{},
		delimiter:		default_delimiter,
		sorted:				[]string{},
	}
	c.ExtractKeys()
	c.ConvertJSON()
	if c.err != nil {
		return c.err
	}

	return nil
}


// Convenience type for cutting down on error checking and type conversion
// boilerplate code during repetative writes.
type errWriter struct {
	w		io.Writer
	err	error
}

func (ew *errWriter) write(value interface{}) {
	if ew.err != nil {
		return
	}
	_, ew.err = ew.w.Write([]byte(toString(value)))
}



// Encapsulates the state necessary to convert JSON input to CSV output.
// 
// This implementation passes over input data twice to first extract all
// possible field names, then to output CSV data. This trades greater time
// complexity for less space complexity.
type converter struct {
	Source				io.ReadSeeker
	Destination		io.Writer
	Keys					map[string]int64
	delimiter			string
	err						error
	sorted				[]string
}


// Extracts all property names from JSON input.
func (c *converter) ExtractKeys() {
	dec := json.NewDecoder(c.Source)

	// Opening bracket
	if _, err := dec.Token(); err != nil {
		c.err = fmt.Errorf("malformed JSON: document must be a single array of objects")
		return
	}

	// Scan each record and extract key names and frequencies
	for dec.More() {
		var record interface{}
		if err := dec.Decode(&record); err != nil {
			c.err = err
			return
		} else {
			m := record.(map[string]interface{})
			for key, _ := range m {
				if _, ok := c.Keys[key]; ok == false {
					c.Keys[key] = 0
				}
				c.Keys[key] += 1
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

	// Extract raw keys and sort them by frequency
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
func (c *converter) ConvertJSON() {
	if c.err != nil {
		return
	}
	if len(c.sorted) == 0 {
		return
	}

	w := errWriter{ w:c.Destination }

	// Write field headers
	w.write(fmt.Sprintf("%s\n", strings.Join(c.sorted, c.delimiter)))

	dec := json.NewDecoder(c.Source)

	// Opening bracket
	if _, err := dec.Token(); err != nil {
		c.err = fmt.Errorf("malformed JSON: document must be a single array of objects")
		return
	}

	// Scan each record and extract key names and frequencies
	for dec.More() {
		var record interface{}
		if err := dec.Decode(&record); err != nil {
			c.err = err
			return
		} else {
			m := record.(map[string]interface{})

			// Write first value (for delimiter reasons)
			if value, ok := m[c.sorted[0]]; ok == true {
				w.write(value)
			}

			// Write subsequent values
			for _, key := range c.sorted[1:] {
				var value interface{} = ""
				if _, ok := m[key]; ok == true {
					value = m[key]
				}
				w.write(c.delimiter)
				w.write(value)
			}

			// Finish off line
			w.write("\n")
		}
	}
	if w.err != nil {
		c.err = fmt.Errorf("output write failure: %s", w.err)
		return
	}

	// Closing bracket
	if _, err := dec.Token(); err != nil {
		c.err = fmt.Errorf("malformed JSON: array does not end properly")
		return
	}
}


func (c converter) Len() int { return len(c.sorted) }
func (c converter) Swap(i, j int) { c.sorted[i], c.sorted[j] = c.sorted[j], c.sorted[i] }
func (c converter) Less(i, j int) bool {
	a, b := c.Keys[c.sorted[i]], c.Keys[c.sorted[j]]
	if a != b {
		return a > b
	} else {
		return c.sorted[j] > c.sorted[i]
	}
}


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
