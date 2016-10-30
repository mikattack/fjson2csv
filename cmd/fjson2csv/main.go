package main

import (
	"flag"
	"fmt"
	"os"

	"gitlab.com/mikattack/fjson2csv"
)

var (
	help              = flag.Bool("h", false, "Usage instructions")
	unbuffered        = flag.Bool("u", false, "Enable unbuffered conversion")
	version    string = "1.0"
	usage      string = `fjson2csv (v%s)

Converts a collection of flat, heterogeneous records from JSON format into
CSV format, writing the results to the given output file.

Usage:
	fjson2csv [input] [output]

Options
	-u  Enable unbuffered conversion
	-h  This help menu

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

	if *unbuffered {
		err = fjson2csv.UnbufferedConvert(src, dst)
	} else {
		err = fjson2csv.BufferedConvert(src, dst)
	}
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		os.Exit(1)
	}
}
