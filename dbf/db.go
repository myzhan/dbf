package dbf

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	// init pq lib
	_ "github.com/lib/pq"
	// init mysql lib
	_ "github.com/go-sql-driver/mysql"
)

func getPostgreDSN(dbHost string, dbPort int, dbUser, dbPassword, dbName string) string {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", dbHost, dbPort, dbUser, dbPassword, dbName)
	return dsn
}

func getMySQLDSN(dbHost string, dbPort int, dbUser, dbPassword, dbName string) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
	return dsn
}

func getDSN(dbType, dbHost string, dbPort int, dbUser, dbPassword, dbName string) string {
	if dbType == "postgres" {
		return getPostgreDSN(dbHost, dbPort, dbUser, dbPassword, dbName)
	} else if dbType == "mysql" {
		return getMySQLDSN(dbHost, dbPort, dbUser, dbPassword, dbName)
	} else {
		log.Fatalf("Wrong db type: %s\n", dbType)
		return ""
	}
}

var dbInstance *sql.DB
var once sync.Once

func getSharedDB() *sql.DB {
	if dbInstance == nil {
		log.Fatalln("DB instance is nil, call initDB first.")
	}
	return dbInstance
}

func initDB(dbType, dbHost string, dbPort int, dbUser, dbPassword, dbName string) {
	once.Do(func() {
		dsn := getDSN(dbType, dbHost, dbPort, dbUser, dbPassword, dbName)
		db, err := sql.Open(dbType, dsn)
		if err != nil {
			log.Fatalf("Failed to connect to db, %v\n", err)
		}
		log.Printf("Connected to %s instance %s:%d\n", dbType, dbHost, dbPort)
		dbInstance = db
	})
}
