package dbf

import (
	"fmt"
	"log"

	"encoding/json"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"database/sql"
)

const dumpSchemaSQL = `select column_name, data_type, ordinal_position from information_schema.columns where table_name='%s' order by ordinal_position;`
const pgPrepareStatementPlaceholder = "$"
const mySQLPrepareStatementPlaceholder = "?"

func dumpSchema(table string) {

	schemaFile := fmt.Sprintf("%s_schema.json", table)
	if _, err := os.Stat(schemaFile); err == nil {
		log.Printf("%s already exists, do nothing and quit now.\n", schemaFile)
		log.Printf("If you want to dump shema of %s, delete or backup %s, and rerun this command.\n", table, schemaFile)
		os.Exit(1)
	}

	db := getSharedDB()
	defer db.Close()

	sql := fmt.Sprintf(dumpSchemaSQL, table)
	rows, err := db.Query(sql)

	if err != nil {
		log.Printf("Error dumping schema, %v\n", err)
	}

	columns := make(map[string]interface{})
	columns["columns"] = make([]*Column, 0)

	//TODO: find an elegant way to find out empty result set.

	for rows.Next() {
		var columnName string
		var dataType string
		var ordinalPosition int
		err = rows.Scan(&columnName, &dataType, &ordinalPosition)

		if err != nil {
			log.Printf("Query error: %v\n", err)
		}

		column := &Column{
			Name:     columnName,
			DataType: dataType,
			Ordinal:  ordinalPosition,
			Mutator: &Mutator{
				Name:   "",
				Params: make(map[string]interface{}),
			},
		}

		columns["columns"] = append(columns["columns"].([]*Column), column)

	}

	bytes, err := json.MarshalIndent(columns, "", "  ")
	filename := fmt.Sprintf("%s_schema.json", table)

	writeTo(bytes, filename)

}

func loadSchema(table string) *Schema {
	filename := fmt.Sprintf("%s_schema.json", table)

	content, err := readFrom(filename)
	if err != nil {
		log.Printf("Error reading %s, %v\n", filename, err)
		os.Exit(1)
	}

	schema := new(Schema)
	err = json.Unmarshal(content, &schema)
	if err != nil {
		log.Printf("Error unmarshaling %s, %v\n", content, err)
		os.Exit(1)
	}

	return schema
}

func filterColumnsWithMutator(columns []*Column) (columnsWithMutator []*Column) {
	columnsWithMutator = make([]*Column, 0)
	for _, v := range columns {
		mutator := v.Mutator
		if mutator.Name != "" {
			columnsWithMutator = append(columnsWithMutator, v)
		}
	}

	if len(columnsWithMutator) == 0 {
		log.Fatalln("The number of columns with mutator is 0, at least one columns with mutator")
	}
	return columnsWithMutator
}

func insertWorker(db *sql.DB, sql string, columnsWithMutator []*Column, countDownLatch *sync.WaitGroup, totalThresHold, finished *int64) {

	stmt, err := db.Prepare(sql)
	defer stmt.Close()

	if err != nil {
		log.Fatalf("Failed to create statement, %v\n", err)
	}

	for {
		number := atomic.AddInt64(totalThresHold, -1)

		if number < 0 {
			countDownLatch.Done()
			return
		}

		mutatedResult := make([]interface{}, len(columnsWithMutator))
		for index, col := range columnsWithMutator {
			mutatorName := col.Mutator.Name
			mutatedResult[index] = resolveMutatorAndExec(mutatorName, col.Mutator.Params)
		}

		_, err = stmt.Exec(mutatedResult...)
		if err != nil {
			log.Printf("Error executing statement, %v\n", err)
			os.Exit(1)
		}

		atomic.AddInt64(finished, 1)
	}
}

func insertData(dbType string, table string, concurrency int, total int64) {

	schema := loadSchema(table)
	columnsWithMutator := filterColumnsWithMutator(schema.Columns)

	sqlTpl := "insert into %s(%s) values(%s);"

	columnNames := make([]string, len(columnsWithMutator))
	placeholders := make([]string, len(columnsWithMutator))
	for index, v := range columnsWithMutator {
		columnNames[index] = v.Name
		if dbType == "postgres" {
			// PostgreSQL uses $1, $2, $3 as placeholder
			placeholders[index] = fmt.Sprintf("$%d", index+1)
		} else {
			// MySQL uses ? as placeholder
			placeholders[index] = "?"
		}
	}

	sqlTpl = fmt.Sprintf(sqlTpl, table, strings.Join(columnNames, ","), strings.Join(placeholders, ","))

	db := getSharedDB()
	txn, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to start a transcation, %v\n", err)
	}

	log.Println("Transcation started")

	countDownLatch := sync.WaitGroup{}
	countDownLatch.Add(concurrency)

	finished := int64(0)
	totalThresHold := int64(0)
	atomic.StoreInt64(&totalThresHold, total)
	atomic.StoreInt64(&finished, 0)

	for i := 0; i < concurrency; i++ {
		go insertWorker(db, sqlTpl, columnsWithMutator, &countDownLatch, &totalThresHold, &finished)
	}
	go func() {
		tps := int64(0)
		for {
			time.Sleep(time.Second)
			log.Printf("Concurrency: %d, TPS: %d, Total: %d, Finished: %d, Left:%d\n", concurrency, finished-tps, total, finished, total-finished)
			tps = finished
		}
	}()
	countDownLatch.Wait()

	err = txn.Commit()
	if err != nil {
		log.Fatalf("Failed to commit transcation, %v\n", err)
	}

	log.Println("Transcation ended")
	log.Println("Done")
}

// Run dbf
func Run() {

	dbType := globalConf.Type
	dbHost := globalConf.Host
	dbPort := globalConf.Port
	dbUser := globalConf.User
	dbPassword := globalConf.Password
	dbName := globalConf.Name
	op := globalConf.OP
	table := globalConf.Table
	concurrency := globalConf.Concurrency
	total := globalConf.Total

	initDB(dbType, dbHost, dbPort, dbUser, dbPassword, dbName)

	switch op {
	case "dump":
		dumpSchema(table)
	case "insert":
		insertData(dbType, table, concurrency, total)
	default:
		log.Println("Wrong op, only dump or insert is supported")
	}
}

func init() {
	// log with file line
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
