package main

import (
	"flag"
	"fmt"
	"os"

	"gitlab.com/mikattack/fjson2csv"
)

var (
	help              = flag.Bool("h", false, "Usage instructions")
	incremental       = flag.Bool("i", false, "Enable incremental conversion")
	readBuffer				= flag.Int("r", 1024, "Internal read buffer size")
	writeBuffer				= flag.Int("w", 1024, "Internal write buffer size")
	version    string = "1.0"
	usage      string = `fjson2csv (v%s)

Converts a collection of flat, heterogeneous records from JSON format into
CSV format, writing the results to the given output file.

By default, the conversion loads the entire file into memory. Use the '-u'
option to convert very large files incrementally.

Usage:
  fjson2csv [input] [output]

Options
  -h  This help menu
  -i  Enable incremental conversion
  -r  Set internal read buffer size in KB (default: 1024)
  -w  Set internal write buffer size in KB (default: 1024)

`
)

func main() {
	flag.Parse()

	var (
		src *os.File
		dst *os.File
		err error
	)

	if *help {
		fmt.Printf(usage, version)
		os.Exit(0)
	}
	if len(os.Args) < 3 {
		fmt.Printf("Missing JSON input filename\n")
		os.Exit(1)
	}

	files := os.Args[len(os.Args) - 2:]
	inputfile  := string(files[0])
	outputfile := string(files[1])

	src, err = os.Open(inputfile)
	if err != nil {
		fmt.Printf("Failed to read JSON input data: %s\n", err.Error())
		os.Exit(1)
	}
	defer src.Close()

	dst, err = os.Create(outputfile)
	if err != nil {
		fmt.Printf("Failed open CSV output file for writing: %s\n", err.Error())
		os.Exit(1)
	}
	defer dst.Close()

	opts := fjson2csv.Options{
		ReadBufferSize:		*readBuffer,
		WriteBufferSize:	*writeBuffer,
	}

	if *incremental {
		err = fjson2csv.UnbufferedConvert(src, dst, opts)
	} else {
		err = fjson2csv.BufferedConvert(src, dst, opts)
	}
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		os.Exit(1)
	}
}
