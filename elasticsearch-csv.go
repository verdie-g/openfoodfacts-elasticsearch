package main

import (
	"context"
	"encoding/csv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/olivere/elastic"
)

const (
	esIndex  = "off"
	esType   = "products"
	bulkSize = 10000
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("usage ./%v off.csv mapping.json\n", os.Args[0])
	}

	filepath := os.Args[1]
	f, err := os.Open(filepath)
	check(err)

	client, err := elastic.NewClient()
	check(err)

	err = recreateIndex(client, esIndex)
	check(err)

	err = putMappingFromFile(client, esIndex, esType, os.Args[2])
	check(err)

	bulkReq := client.Bulk()

	records := make(chan []string)
	go readCsv(f, records)

	<-records
	n := 1
	for record := range records {
		n++
		p := ProductFromCsvRecord(record, n)
		if p == nil {
			continue
		}
		req := elastic.NewBulkIndexRequest().Index(esIndex).Type(esType).Id(strconv.Itoa(n)).Doc(p)
		bulkReq = bulkReq.Add(req)
		if n%bulkSize == 0 {
			_, err = bulkReq.Do(context.Background())
			check(err)
		}
	}
	_, err = bulkReq.Do(context.Background())
	check(err)
}

func readCsv(f *os.File, records chan []string) {
	r := csv.NewReader(f)
	r.Comma = '\t'
	r.LazyQuotes = true

	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			if err, ok := err.(*csv.ParseError); ok {
				switch err.Err {
				case csv.ErrFieldCount:
					log.Println(err)
					continue
				default:
					panic(err)
				}
			}
		}

		records <- record
	}

	close(records)
}

func recreateIndex(client *elastic.Client, index string) error {
	client.DeleteIndex(index).Do(context.Background())
	_, err := client.CreateIndex(index).Do(context.Background())
	return err
}

func putMappingFromFile(client *elastic.Client, index string, t string, mappingFile string) error {
	mappingBuf, err := ioutil.ReadFile(mappingFile)
	mapping := string(mappingBuf)
	_, err = client.PutMapping().Index(index).Type(t).BodyString(mapping).Do(context.Background())
	return err
}
