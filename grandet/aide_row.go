package grandet

import (
	"encoding/base64"
	"fmt"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/schema"
)

const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"
	AlterAction  = "alter" // add
)

type QueryEvent struct {
	Table  *schema.Table
	Action string

	Query []byte
}

// add
func newQueryEvent(table *schema.Table, action string, query []byte) *QueryEvent {
	e := new(QueryEvent)

	e.Table = table
	e.Action = action
	e.Query = query

	return e
}

type RowsEvent struct {
	Table  *schema.Table
	Action string
	// changed row list
	// binlog has three update event version, v0, v1 and v2.
	// for v1 and v2, the rows number must be even.
	// Two rows for one event, format is [before update row, after update row]
	// for update v0, only one row for a event, and we don't support this version.
	Rows [][]interface{}
}

func newRowsEvent(table *schema.Table, action string, rows [][]interface{}) *RowsEvent {
	e := new(RowsEvent)

	e.Table = table
	e.Action = action
	e.Rows = rows

	return e
}

// Get primary keys in one row for a table, a table may use multi fields as the PK
func GetPKValues(table *schema.Table, row []interface{}) ([]interface{}, error) {
	indexes := table.PKColumns
	if len(indexes) == 0 {
		return nil, errors.Errorf("table %s has no PK", table)
	} else if len(table.Columns) != len(row) {
		return nil, errors.Errorf("table %s has %d columns, but row data %v len is %d", table,
			len(table.Columns), row, len(row))
	}

	values := make([]interface{}, 0, len(indexes))

	for _, index := range indexes {
		values = append(values, row[index])
	}

	return values, nil
}

// String implements fmt.Stringer interface.
func (r *RowsEvent) String() string {
	return fmt.Sprintf("%s %s %v", r.Action, r.Table, r.Rows)
}

func DeBase64RowsEvent(e RowsEvent) RowsEvent {

	en := RowsEvent{}

	for i := 0; i < len(e.Rows); i++ {
		row := make([]interface{}, 0)
		for k := 0; k < len(e.Table.Columns); k++ {

			if e.Rows[i][k] == nil {
				row = append(row, nil)
			} else {
				value := InterfaceToString(e.Rows[i][k])
				valuep, _ := base64.StdEncoding.DecodeString(value)
				row = append(row, string(valuep))

			}
		}
		en.Rows = append(en.Rows, row)
	}

	en.Action = e.Action
	en.Table = e.Table

	return en
}

func EscapeBytesBackslash(v []byte) string {
	pos := 0
	buf := make([]byte, len(v)*2)
	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return string(buf[:pos])
}

func EscapeStringBackslash(v string) string {
	pos := 0
	buf := make([]byte, len(v)*2)
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return string(buf[:pos])
}
