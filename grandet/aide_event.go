package grandet

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/schema"
)

func Do(e *RowsEvent) string {

	var cmd string

	for i := 0; i < len(e.Rows); i++ {
		var err error

		if e.Action == InsertAction {
			cmd, err = insert(e.Table, e.Rows[i])
		} else if e.Action == UpdateAction {
			cmd, err = update(e.Table, e.Rows[i], e.Rows[i+1])
			i += 1
		} else if e.Action == DeleteAction {
			cmd, err = delete(e.Table, e.Rows[i])
		} else {
			return "Do: Have No Match Action \n"
		}

		if err != nil {
			log.Errorf("handle data err: %v", err)
			return "Do: Have No Statement \n"
		}
	}

	return cmd
}

func insert(table *schema.Table, row []interface{}) (string, error) {
	var columns, values string
	for k := 0; k < len(table.Columns); k++ {
		columns += "`" + table.Columns[k].Name + "`,"
		if row[k] == nil {
			values += "NULL,"
		} else {
			values += "'" + EscapeStringBackslash(InterfaceToStringRawType(row[k],table.Columns[k].RawType)) + "',"
		}
	}
	if columns == "" || values == "" {
		log.Infof("insert is empty: %s %s", columns, values)
		return "", nil
	}
	columns = columns[0 : len(columns)-1]
	values = values[0 : len(values)-1]

	sqlcmd := "REPLACE INTO `" + table.Schema + "`.`" + table.Name + "` (" + columns + ") VALUES (" + values + ")"

	return sqlcmd, nil
}

func delete(table *schema.Table, row []interface{}) (string, error) {
	var condition string
	for _, k := range table.PKColumns {
		if row[k] == nil {
			condition += "`" + table.Columns[k].Name + "`=NULL AND "
		} else {
			condition += "`" + table.Columns[k].Name + "`='" + EscapeStringBackslash(InterfaceToStringRawType(row[k],table.Columns[k].RawType)) + "' AND "
		}
	}

	if condition == "" {
		log.Warnf("delete condition is empty ignore....")
		return "", nil
	}
	condition = condition[0 : len(condition)-len(" AND ")]

	sqlcmd := "DELETE FROM `" + table.Schema + "`.`" + table.Name + "` WHERE " + condition

	return sqlcmd, nil
}

func update(table *schema.Table, before, after []interface{}) (string, error) {
	var condition, setValues string
	for _, k := range table.PKColumns {
		if before[k] == nil {
			condition += "`" + table.Columns[k].Name + "`=NULL AND "
		} else {
			condition += "`" + table.Columns[k].Name + "`='" +
				EscapeStringBackslash(InterfaceToStringRawType(before[k],table.Columns[k].RawType)) + "' AND "
		}
	}

	if condition == "" {
		log.Warnf("update condition is empty ignore....")
		return "", nil
	}
	condition = condition[0 : len(condition)-len(" AND ")]

	for k := 0; k < len(table.Columns); k++ {
		if after[k] == nil {
			setValues += "`" + table.Columns[k].Name + "`=NULL,"
		} else {
			setValues += "`" + table.Columns[k].Name + "`='" + EscapeStringBackslash(InterfaceToStringRawType(after[k],table.Columns[k].RawType)) + "',"
		}
	}
	setValues = setValues[0 : len(setValues)-1]

	sqlcmd := "UPDATE `" + table.Schema + "`.`" + table.Name + "` SET" + setValues + " WHERE " + condition

	return sqlcmd, nil
}
