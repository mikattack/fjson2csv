package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6
	letterIdxMask = 1<<letterIdxBits - 1
	letterIdxMax  = 63 / letterIdxBits

	str = iota
	number
)

var (
	fields = flag.Int("f", 10, "Maximum number of generated fields")
	help   = flag.Bool("h", false, "Help menu")
	size   = flag.Int("m", 10, "Approximate size of generated JSON file")

	version string = "1.0"
	usage   string = `fjson2csv-data (v%s)

Generates sample JSON data for testing with 'fjson2csv'. JSON formatted data
is sent to STDOUT.

Usage:
	fjson2csv

Options
	-f  Maximum number of fields to generate (min: 1, max: 20, default:10)
	-m  Approximate size (in MB) of generated JSON file (default: 10)
	-h  This help menu

`
)

func main() {
	flag.Parse()

	if *help {
		fmt.Printf(usage, version)
		os.Exit(0)
	}
	if *fields < 1 {
		*fields = 1
	}
	if *fields > 20 {
		*fields = 20
	}
	if *size < 1 {
		*size = 1
	}

	/*
		preamble := `
	Generating data:
	- Size:      %dmb
	- Fields:    %d

	`
		fmt.Printf(preamble, *dir, os.Args[1], *dir, os.Args[1], *size, *fields)
	*/

	opts := options{
		Stream: os.Stdout,
		Size:   *size,
		Fields: *fields,
	}
	g := newGenerator(&opts)
	g.GenerateHeaders()
	g.GenerateData()
	if g.err != nil {
		fmt.Printf("generation failed: %s\n")
	}
}

type errWriter struct {
	w   io.Writer
	err error
}

func (ew *errWriter) write(value interface{}) int64 {
	var written int
	if ew.err != nil {
		return 0
	}
	written, ew.err = ew.w.Write([]byte(toString(value)))
	return int64(written)
}
func (ew *errWriter) writeBytes(value []byte) int64 {
	var written int
	if ew.err != nil {
		return 0
	}
	written, ew.err = ew.w.Write(value)
	return int64(written)
}

type options struct {
	Stream io.Writer
	Size   int
	Fields int
}

type generator struct {
	Headers []string
	Options *options
	err     error
	stream  errWriter
	source  rand.Source
}

func newGenerator(opts *options) generator {
	return generator{
		Headers: []string{},
		Options: opts,
		stream:  errWriter{w: opts.Stream},
		source:  rand.NewSource(time.Now().UnixNano()),
	}
}

func (g *generator) GenerateHeaders() {
	// TODO: Allow custom header fields
	for i := 0; i < g.Options.Fields; i++ {
		field := randomString(g.source, rand.Intn(10)+5)
		g.Headers = append(g.Headers, field)
	}
}

func (g *generator) GenerateData() {
	// Only useful for testing (eventually?)
	if len(g.Headers) == 0 {
		g.err = fmt.Errorf("no headers specified")
		return
	}

	limit := int64(g.Options.Size * 1048576)
	//limit := int64(g.Options.Size * 1000)
	written := int64(0)

	// Write the first record
	written += g.stream.write("[\n")
	record := generateRecord(g)
	if encoded, err := json.Marshal(record); err != nil {
		g.err = err
		return
	} else {
		written += g.stream.writeBytes(encoded)
		written += g.stream.write("\n")
	}

	// Write subsequent records
	for written < limit {
		// This is slow, but I didn't wanna figure out the streaming encoder
		record = generateRecord(g)
		if encoded, err := json.Marshal(record); err != nil {
			g.err = err
			return
		} else {
			written += g.stream.write(",")
			written += g.stream.writeBytes(encoded)
			written += g.stream.write("\n")
		}
	}
	g.stream.write("]\n")
}

// Generate a random string of a fixed length.
//
// From: http://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
func randomString(src rand.Source, n int) string {
	buffer := make([]byte, n)
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			buffer[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(buffer)
}

// Generates random data of a random type.
func randomData(src rand.Source, datatype int) interface{} {
	switch datatype {
	case 0:
		return randomString(src, rand.Intn(10)+5)
	case 1:
		return rand.Intn(41) + 1
	default:
		return (rand.Intn(1) == 1)
	}
}

func generateRecord(g *generator) map[string]interface{} {
	m := map[string]interface{}{}
	f := rand.Intn(len(g.Headers) - 1)
	fields := rand.Intn(g.Options.Fields-1) + 1
	for i := 0; i < fields; i++ {
		m[g.Headers[f]] = randomData(g.source, f%3)
		n := rand.Intn(len(g.Headers) - 1)
		for n == f {
			n = rand.Intn(len(g.Headers) - 1)
		}
		f = n
	}
	return m
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
