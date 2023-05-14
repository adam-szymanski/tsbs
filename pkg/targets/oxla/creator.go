package oxla

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/timescale/tsbs/pkg/targets"

	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	tagsKey      = "tags"
	TimeValueIdx = "TIME-VALUE"
	ValueTimeIdx = "VALUE-TIME"
)

// allows for testing
var fatal = log.Fatalf

var tableCols = make(map[string][]string)

type dbCreator struct {
	driver  string
	ds      targets.DataSource
	connStr string
	opts    *LoadingOptions
}

func (d *dbCreator) Init() {
	// read the headers before all else
	d.ds.Headers()
	d.connStr = d.opts.GetConnectString()
}

// MustConnect connects or exits on errors
func MustConnect(dbType, connStr string) *sql.DB {
	db, err := sql.Open(dbType, connStr)
	if err != nil {
		panic(err)
	}
	return db
}

func (d *dbCreator) DBExists(dbName string) bool {
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	log.Println("RemoveOldDB")
	db := MustConnect(d.driver, d.connStr)
	defer db.Close()
	MustExec(db, "DROP SCHEMA "+dbName+" CASCADE;")
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	db := MustConnect(d.driver, d.connStr)
	MustExec(db, "CREATE SCHEMA "+dbName)
	db.Close()
	return nil
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	dbBench := MustConnect(d.driver, d.opts.GetConnectString())
	defer dbBench.Close()

	headers := d.ds.Headers()
	tagNames := headers.TagKeys
	tagTypes := headers.TagTypes
	if d.opts.CreateMetricsTable {
		createTagsTable(dbName, dbBench, tagNames, tagTypes, d.opts.UseJSON)
	}
	// tableCols is a global map. Globally cache the available tags
	tableCols[tagsKey] = tagNames
	// tagTypes holds the type of each tag value (as strings from Go types (string, float32...))
	d.opts.TagColumnTypes = tagTypes

	// Each table is defined in the dbCreator 'cols' list. The definition consists of a
	// comma separated list of the table name followed by its columns. Iterate over each
	// definition to update our global cache and create the requisite tables and indexes
	for tableName, columns := range headers.FieldKeys {
		// tableCols is a global map. Globally cache the available columns for the given table
		tableCols[tableName] = columns
		fieldDefs, indexDefs := d.getFieldAndIndexDefinitions(tableName, columns)
		if d.opts.CreateMetricsTable {
			d.createTableAndIndexes(dbName, dbBench, tableName, fieldDefs, indexDefs)
		} else {
			// If not creating table, wait for another client to set it up
			i := 0
			checkTableQuery := fmt.Sprintf("SELECT * FROM system.tables WHERE name = '%s' AND namespace_name = '%s'", tableName, dbName)
			r := MustQuery(dbBench, checkTableQuery)
			for !r.Next() {
				time.Sleep(100 * time.Millisecond)
				i += 1
				if i == 600 {
					return fmt.Errorf("expected table not created after one minute of waiting")
				}
				r = MustQuery(dbBench, checkTableQuery)
			}
			return nil
		}
	}
	return nil
}

// getFieldAndIndexDefinitions iterates over a list of table columns, populating lists of
// definitions for each desired field and index. Returns separate lists of fieldDefs and indexDefs
func (d *dbCreator) getFieldAndIndexDefinitions(tableName string, columns []string) ([]string, []string) {
	var fieldDefs []string
	var indexDefs []string
	var allCols []string

	partitioningField := tableCols[tagsKey][0]
	// If the user has specified that we should partition on the primary tags key, we
	// add that to the list of columns to create
	if d.opts.InTableTag {
		allCols = append(allCols, partitioningField)
	}

	allCols = append(allCols, columns...)
	for idx, field := range allCols {
		if len(field) == 0 {
			continue
		}
		fieldType := "DOUBLE PRECISION"
		// This condition handles the case where we keep the primary tag key in the table
		// and partition on it. Since under the current implementation this tag is always
		// hostname, we set it to a TEXT field instead of DOUBLE PRECISION
		if d.opts.InTableTag && idx == 0 {
			fieldType = "TEXT"
		}

		fieldDefs = append(fieldDefs, fmt.Sprintf("\"%s\" %s", field, fieldType))
		// If the user specifies indexes on additional fields, add them to
		// our index definitions until we've reached the desired number of indexes
	}
	return fieldDefs, indexDefs
}

// createTableAndIndexes takes a list of field and index definitions for a given tableName and constructs
// the necessary table, index, and potential hypertable based on the user's settings
func (d *dbCreator) createTableAndIndexes(dbName string, dbBench *sql.DB, tableName string, fieldDefs []string, indexDefs []string) {
	// We default to the tags_id column unless users are creating the
	// name/hostname column in the time-series table for multi-node
	// testing. For distributed queries, pushdown of JOINs is not yet
	// supported.
	var partitionColumn string = "tags_id"

	if d.opts.InTableTag {
		partitionColumn = tableCols[tagsKey][0]
	}

	MustExec(dbBench, fmt.Sprintf("CREATE TABLE %s.\"%s\" (\"time\" timestamp, tags_id integer, %s, additional_tags JSONB)", dbName, tableName, strings.Join(fieldDefs, ",")))
	if d.opts.PartitionIndex {
		MustExec(dbBench, fmt.Sprintf("CREATE INDEX ON %s.\"%s\"(%s, \"time\" DESC)", dbName, tableName, partitionColumn))
	}

	// Only allow one or the other, it's probably never right to have both.
	// Experimentation suggests (so far) that for 100k devices it is better to
	// use --time-partition-index for reduced index lock contention.
	if d.opts.TimePartitionIndex {
		MustExec(dbBench, fmt.Sprintf("CREATE INDEX ON %s.\"%s\"(\"time\" DESC, %s)", dbName, tableName, partitionColumn))
	} else if d.opts.TimeIndex {
		MustExec(dbBench, fmt.Sprintf("CREATE INDEX ON %s.\"%s\"(\"time\" DESC)", dbName, tableName))
	}

	for _, indexDef := range indexDefs {
		MustExec(dbBench, indexDef)
	}
}

func createTagsTable(dbName string, db *sql.DB, tagNames, tagTypes []string, useJSON bool) {
	db.Exec("DROP TABLE IF EXISTS " + dbName + ".tags")
	if useJSON {
		MustExec(db, "CREATE TABLE "+dbName+".tags(id INTEGER NOT NULL, tagset JSONB)")
		return
	}

	MustExec(db, generateTagsTableQuery(dbName, tagNames, tagTypes))
}

func generateTagsTableQuery(dbName string, tagNames, tagTypes []string) string {
	tagColumnDefinitions := make([]string, len(tagNames))
	for i, tagName := range tagNames {
		pgType := serializedTypeToPgType(tagTypes[i])
		tagColumnDefinitions[i] = fmt.Sprintf("%s %s", tagName, pgType)
	}

	cols := strings.Join(tagColumnDefinitions, ", ")
	return fmt.Sprintf("CREATE TABLE %s.tags(id INTEGER, %s)", dbName, cols)
}

// MustExec executes query or exits on error
func MustExec(db *sql.DB, query string, args ...interface{}) sql.Result {
	r, err := db.Exec(query, args...)
	if err != nil {
		if len(query) > 100 {
			query = query[:100]
		}
		fmt.Printf("could not execute sql: %s", query)
		panic(err)
	}
	return r
}

// MustQuery executes query or exits on error
func MustQuery(db *sql.DB, query string, args ...interface{}) *sql.Rows {
	r, err := db.Query(query, args...)
	if err != nil {
		panic(err)
	}
	return r
}

// MustBegin starts transaction or exits on error
func MustBegin(db *sql.DB) *sql.Tx {
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	return tx
}

func serializedTypeToPgType(serializedType string) string {
	switch serializedType {
	case "string":
		return "TEXT"
	case "float32":
		return "FLOAT"
	case "float64":
		return "DOUBLE PRECISION"
	case "int64":
		return "BIGINT"
	case "int32":
		return "INTEGER"
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}
