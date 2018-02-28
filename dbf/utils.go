package dbf

import (
	"io/ioutil"
	"log"
)

func pow(n, m int) int {

	result := 1

	for i := 0; i < m; i++ {
		result = result * n
	}

	return result

}

func readFrom(filename string) (content []byte, err error) {
	log.Printf("Reading from %s\n", filename)
	content, err = ioutil.ReadFile(filename)
	return content, err
}

func writeTo(content []byte, filename string) {
	log.Printf("Writting to %s\n", filename)
	ioutil.WriteFile(filename, content, 0644)
}
