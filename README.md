# fjson2csv

Converts a collection of flat, heterogeneous records from JSON format into CSV format, writing the results to `STDOUT`.

Can be used as a library or command line tool.


## Installation

Use `go get`:

```
go get gitlab.com/mikattack/fjson2csv
cd $GOPATH/src/gitlab.com/mikattack/fjson2csv
make install
```


## Usage

Given the following JSON document:

```json
[
  {
    "id": 1,
    "first_name": "Jane",
    "last_name": "Doe"
  },
  {
    "id": 2,
    "first_name": "John",
    "middle_initial": "Q",
    "last_name": "Public",
    "birth_year": 1971
  },
  {
    "id": 3,
    "anonymous_user": true,
    "crm_id": "abc123"
  },
  {
    "id": 4,
    "first_name": "Albert",
    "last_name": "Einstein",
    "profession": "Scientist",
    "birth_year": 1879,
    "e_equals_mc_squared": true
  }
]
```

This is what an example conversion looks like:

```sh
$: fjson2csv example.json

id,first_name,last_name,birth_year,anonymous_user,crm_id,e_equals_mc_squared,middle_initial,profession
1,Jane,Doe,,,,,,
2,John,Public,1971,,,,Q,
3,,,,true,abc123,,,
4,Albert,Einstein,1879,,,true,,Scientist
```


## Notes

This is a special-case tool which makes several assumptions during the conversion process:

- Input JSON is a single collection (array) of objects
- Each object contains only properties with scalar values (no nested objects)
- No expected consistency of property names from object to object (eg. no fixed schema)
- No string values of properties contain a CSV delimiter (a comma, by default)
- CSV headers are always included
- **All** properties are included in CSV output, even if an object is missing them
- CSV fields are sorted by their frequency, then alphabetically


## License

BSD


## Author

Alex Mikitik