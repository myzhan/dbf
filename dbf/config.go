package dbf

import (
	"encoding/json"
	"flag"
	"log"
	"os"
)

// Config holds all configurations.
type Config struct {
	Type        string `json:"dbType"`
	Host        string `json:"dbHost"`
	Port        int    `json:"dbPort"`
	Name        string `json:"dbName"`
	User        string `json:"dbUser"`
	Password    string `json:"dbPassword"`
	OP          string `json:"op"`
	Table       string `json:"table"`
	Concurrency int    `json:"concurrency"`
	Total       int64  `json:"total"`
}

// NewDefaultConfig return a config instance with default value.
func NewDefaultConfig() *Config {
	return &Config{
		Type:        "postgres",
		Host:        "localhost",
		Port:        5432,
		Concurrency: 1,
	}
}

var globalConf *Config

func init() {

	confPath := flag.String("conf", "conf.json", "Path of configuration file.")
	op := flag.String("op", "", "dump or insert")
	flag.Parse()

	// init globalConf
	globalConf = NewDefaultConfig()

	// load configurations from confPath, defaults to conf.json
	if _, err := os.Stat(*confPath); err == nil {
		fileContent, err := readFrom(*confPath)
		if err != nil {
			log.Printf("Error reading %s, ignored. %v\n", *confPath, err)
		} else {
			err = json.Unmarshal(fileContent, &globalConf)
			if err != nil {
				log.Printf("Error unmarshaling %s, ignored. %v\n", *confPath, err)
			}
		}
	}

	if *op != "" && (*op == "dump" || *op == "insert") {
		globalConf.OP = *op
	}

}
