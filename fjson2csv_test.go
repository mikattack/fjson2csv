package fjson2csv

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"testing"
	"testing/iotest"
)

var (
	rawJson string
	rawCsv  string
)

type badSeeker struct {
	io.Reader
}

func (bs badSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("intentional")
}

func TestBufferedConvert(t *testing.T) {
	t.Parallel()

	// Convert JSON to CSV
	buffer := bytes.Buffer{}
	if err := BufferedConvert(strings.NewReader(rawJson), &buffer, Options{}); err != nil {
		t.Fatalf("conversion failure: %s", err.Error())
	}

	// Compare expected vs. actual
	expected := rawCsv
	actual := buffer.String()
	if actual != rawCsv {
		t.Logf("converted JSON data did not match expected CSV output")
		t.Logf("Expected:\n%s", expected)
		t.Logf("Found:\n%s", actual)
		t.FailNow()
	}
}

func BenchmarkBufferedConvert(b *testing.B) {
	for n := 0; n < b.N; n++ {
		buffer := bytes.Buffer{}
		BufferedConvert(strings.NewReader(rawJson), &buffer, Options{})
	}
}

func TestUnbufferedConvert(t *testing.T) {
	t.Parallel()

	// Convert JSON to CSV
	buffer := bytes.Buffer{}
	if err := UnbufferedConvert(strings.NewReader(rawJson), &buffer, Options{}); err != nil {
		t.Fatalf("conversion failure: %s", err.Error())
	}

	// Compare expected vs. actual
	expected := rawCsv
	actual := buffer.String()
	if actual != rawCsv {
		t.Logf("converted JSON data did not match expected CSV output")
		t.Logf("Expected:\n%s", expected)
		t.Logf("Found:\n%s", actual)
		t.FailNow()
	}
}

func BenchmarkUnbufferedConvert(b *testing.B) {
	for n := 0; n < b.N; n++ {
		buffer := bytes.Buffer{}
		UnbufferedConvert(strings.NewReader(rawJson), &buffer, Options{})
	}
}

func TestToString(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"string", "test", "test"},
		{"float", float64(12345), "12345"},
		{"bool", true, "true"},
		{"bool", false, "false"},
		{"null", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			converted := toString(tc.value)
			if converted != tc.expected {
				t.Errorf("expected '%s', found '%v'", tc.expected, converted)
			}
		})
	}
}

func TestKeySort(t *testing.T) {
	expected := []string{"marbles", "angles", "apples", "colors", "feelings"}
	converter := converter{
		Keys: map[string]int64{
			"apples":   4,
			"angles":   4,
			"marbles":  12,
			"feelings": 1,
			"colors":   1,
		},
	}
	sort.Sort(converter)
	for index, value := range converter.sorted {
		if value != expected[index] {
			t.Logf("key sorting failed")
			t.Logf("Expected:\n%s", expected)
			t.Logf("Found:\n%s", converter.sorted)
			t.FailNow()
		}
	}
}

func TestWriteRecordCallback(t *testing.T) {
	t.Parallel()

	/*
	 * Here, we test the same record twice. One should output the expected CSV
	 * string and the other should not. The failure is simulated with a writer
	 * that stops writing after a few bytes.
	 */

	c := converter{
		sorted:    []string{"name", "category", "age", "valid"},
		delimiter: ",",
	}

	cases := []struct {
		name     string
		expected string
		record   map[string]interface{}
		writer   *errWriter
		willFail bool
	}{
		{
			"failing write",
			"pickle,condiment,4,true,",
			map[string]interface{}{"name": "pickle", "category": "condiment", "age": 4, "valid": true},
			newErrorWriter(iotest.TruncateWriter(&bytes.Buffer{}, 12), default_write_buffer_size*1000),
			//&errWriter{w: iotest.TruncateWriter(&bytes.Buffer{}, 12)},
			true,
		},
		{
			"successful write",
			"pickle,condiment,4,true,",
			map[string]interface{}{"name": "pickle", "category": "condiment", "age": 4, "valid": true},
			newErrorWriter(&bytes.Buffer{}, default_write_buffer_size*1000),
			//&errWriter{w: &bytes.Buffer{}},
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := writeRecord(tc.record, &c, tc.writer)
			if err != nil && tc.willFail == false {
				t.Errorf("failed to write CSV data")
			}
		})
	}
}

func TestExtractKeysCallback(t *testing.T) {
	t.Parallel()

	/*
	 * Here, we test whether the callback function properly increments the
	 * frequency counters of the given index.
	 */

	c := converter{Keys: map[string]int64{}}
	cases := []struct {
		name     string
		expected map[string]int64
		record   map[string]interface{}
	}{
		{
			"full",
			map[string]int64{"name": 1, "category": 1, "age": 1, "valid": 1},
			map[string]interface{}{"name": "pickle", "category": "condiment", "age": 4, "valid": true},
		},
		{
			"partial",
			map[string]int64{"name": 1, "category": 2, "age": 1, "valid": 2},
			map[string]interface{}{"category": "condiment", "valid": true},
		},
		{
			"empty",
			map[string]int64{"name": 1, "category": 2, "age": 1, "valid": 2},
			map[string]interface{}{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			extractKeys(tc.record, &c)
			for key, frequency := range c.Keys {
				if frequency != tc.expected[key] {
					t.Errorf("key frequency mismatch: expected '%d', found '%d'", tc.expected[key], frequency)
					break
				}
			}
		})
	}
}

func TestWriteCSV(t *testing.T) {
	t.Parallel()

	raw := `[
		{"test":"hello", "example":42},
		{"example":12}
	]`

	buffer := bytes.Buffer{}
	reader := strings.NewReader(raw)
	c := converter{
		Source:      reader,
		Destination: &buffer,
		Keys:        map[string]int64{"test": 1, "example": 2},
		delimiter:   ",",
		err:         fmt.Errorf("simulated error"),
		sorted:      []string{},
	}

	// Simulate prior error
	c.WriteCsv(writeRecord)
	if buffer.String() != "" {
		t.Errorf("expected zero output when converter indicates an error")
	}

	// Simulate failed indexing
	c.err = nil
	c.WriteCsv(writeRecord)
	if buffer.String() != "" {
		t.Errorf("expected zero output when converter failed indexing")
	}

	// Simulate a successful setup and conversion
	expected := `example,test
42,hello
12,
`
	c.sorted = []string{"example", "test"}
	c.WriteCsv(writeRecord)
	if buffer.String() != expected {
		t.Logf("accurate CSV conversion unsuccessful")
		t.Logf("Expected:\n%s", expected)
		t.Logf("Found:\n%s", buffer.String())
		t.FailNow()
	}
}

func TestIndexFields(t *testing.T) {
	t.Parallel()

	raw := `[
		{"test":"hello", "example":42},
		{"example":12}
	]`

	buffer := bytes.Buffer{}
	reader := strings.NewReader(raw)
	c := converter{
		Source:      reader,
		Destination: &buffer,
		Keys:        map[string]int64{},
		sorted:      []string{},
	}

	expectedSortOrder := []string{"example", "test"}
	expectedKeyMap := map[string]int64{"test": 1, "example": 2}
	c.IndexFields(extractKeys)

	for index, key := range expectedSortOrder {
		if c.sorted[index] != key {
			t.Logf("key sorting failed")
			t.Logf("Expected:\n%v", expectedSortOrder)
			t.Logf("Found:\n%v", c.sorted)
			t.Fail()
			break
		}
	}

	for key, expectedFrequency := range expectedKeyMap {
		if frequency, ok := c.Keys[key]; ok == false || frequency != expectedFrequency {
			t.Errorf("key frequency mismatch: expected '%d', found '%d'", expectedFrequency, frequency)
			break
		}
	}
}

func TestWalkJsonList(t *testing.T) {
	t.Parallel()

	c := converter{}

	fnSucceed := func(r map[string]interface{}, args ...interface{}) error { return nil }
	fnFail := func(r map[string]interface{}, args ...interface{}) error { return fmt.Errorf("intentional") }

	cases := []struct {
		name     string
		reader   io.ReadSeeker
		fn       walkFunction
		willFail bool
	}{
		{"malformed json", io.ReadSeeker(strings.NewReader(`test":1}]`)), fnSucceed, true},
		{"malformed open bracket", io.ReadSeeker(strings.NewReader(`{"test":1}]`)), fnSucceed, true},
		{"malformed close bracket", io.ReadSeeker(strings.NewReader(`[{"test":1}`)), fnSucceed, true},
		{"bad seek", badSeeker{strings.NewReader(`[{"test":1}]`)}, fnSucceed, true},
		{"bad callback", io.ReadSeeker(strings.NewReader(`[{"test":1}]`)), fnFail, true},
		{"success", io.ReadSeeker(strings.NewReader(`[{"test":1}]`)), fnSucceed, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c.Source = tc.reader
			c.WalkJsonList(tc.fn)
			if c.err != nil && tc.willFail == false {
				t.Fail()
			}
			c.err = nil
		})
	}
}

func readFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file")
	}
	defer file.Close()
	if raw, err := ioutil.ReadAll(file); err != nil {
		return "", fmt.Errorf("failed to read file: %s", err.Error())
	} else {
		return string(raw), nil
	}
}

func TestMain(m *testing.M) {
	flag.Parse()

	var err error
	jsonFile := "./testdata/example.json"
	csvFile := "./testdata/example.csv"

	if rawJson, err = readFile(jsonFile); err != nil {
		panic(fmt.Sprintf("json data: %s", err.Error()))
	}
	if rawCsv, err = readFile(csvFile); err != nil {
		panic(fmt.Sprintf("csv data: %s", err.Error()))
	}

	result := m.Run()
	os.Exit(result)
}
