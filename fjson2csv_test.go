package fjson2csv

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"testing"
	"testing/iotest"
)

type badSeeker struct {
	io.Reader
}

func (bs badSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("intentional")
}

// Functional test case
func TestConvert(t *testing.T) {
	t.Parallel()

	var expected string
	inputFile := "./testdata/example.json"
	outputFile := "./testdata/example.csv"

	// Read in expected CSV output
	file, err := os.Open(outputFile)
	if err != nil {
		t.Fatalf("failed to open expected CSV output file")
	}
	if rawOutput, err := ioutil.ReadAll(file); err != nil {
		t.Fatalf("failed to read expected CSV output file: %s", err.Error())
	} else {
		expected = string(rawOutput)
	}
	file.Close()

	// Open JSON input file
	file, err = os.Open(inputFile)
	if err != nil {
		t.Fatalf("failed to open JSON input file: %s", err.Error())
	}
	defer file.Close()

	// Convert JSON
	buffer := bytes.Buffer{}
	if err := Convert(file, &buffer); err != nil {
		t.Fatalf("failed to open JSON input file: %s", err.Error())
	}

	// Compare expected vs. actual
	actual := buffer.String()
	if actual != expected {
		t.Logf("converted JSON data did not match expected CSV output")
		t.Logf("Expected:\n%s", expected)
		t.Logf("Found:\n%s", actual)
		t.FailNow()
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

	keymap := []string{"name", "category", "age", "valid"}
	delimiter := ","

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
			&errWriter{w: iotest.TruncateWriter(&bytes.Buffer{}, 12)},
			true,
		},
		{
			"successful write",
			"pickle,condiment,4,true,",
			map[string]interface{}{"name": "pickle", "category": "condiment", "age": 4, "valid": true},
			&errWriter{w: &bytes.Buffer{}},
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := writeRecord(tc.record, keymap, delimiter, tc.writer)
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

	keymap := map[string]int64{}
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
			extractKeys(tc.record, keymap)
			for key, frequency := range keymap {
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
	c.WriteCsv()
	if buffer.String() != "" {
		t.Errorf("expected zero output when converter indicates an error")
	}

	// Simulate failed indexing
	c.err = nil
	c.WriteCsv()
	if buffer.String() != "" {
		t.Errorf("expected zero output when converter failed indexing")
	}

	// Simulate a successful setup and conversion
	expected := `example,test
42,hello
12,
`
	c.sorted = []string{"example", "test"}
	c.WriteCsv()
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
	c.IndexFields()

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
