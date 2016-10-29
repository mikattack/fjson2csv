package main

import (
	"flag"
	"fmt"
  "os"

  "gitlab.com/mikattack/fjson2csv"
)


var (
	help = flag.Bool("h", false, "Usage instructions")
	version string = "1.0"
	usage string = `fjson2csv (v%s)

Converts a collection of flat, heterogeneous records from JSON format into
CSV format, writing the results to a STDOUT.

Usage:
	fjson2csv [filename]

`
)


func main() {
	flag.Parse()

  if len(os.Args) < 2 {
  	fmt.Printf("Missing JSON input filename\n")
    os.Exit(1)
  }
  if *help {
  	fmt.Printf(usage, version)
  	os.Exit(0)
  }

  file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Printf("Failed to read JSON input data: %s\n", err.Error())
  	os.Exit(1)
	}
	defer file.Close()

  err = fjson2csv.Convert(file, os.Stdout)
  if err != nil {
  	fmt.Printf("%s\n", err.Error())
  	os.Exit(1)
  }
}