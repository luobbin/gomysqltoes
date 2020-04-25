package main

import (
	"bytes"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
	"strings"
	"time"
)

func get_es(address []string, username, password string) *elasticsearch.Client {
	// Create the Elasticsearch client
	cfg := elasticsearch.Config{
		Addresses: address,
		Username:  username,
		Password:  password,
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}
	return es
}

func save_es_data(tasks chan Chandata) {
	log.SetFlags(0)
	for task := range tasks {
		//返回结果初始化
		type bulkResponse struct {
			Errors bool `json:"errors"`
			Items  []struct {
				Index struct {
					ID     string `json:"_id"`
					Result string `json:"result"`
					Status int    `json:"status"`
					Error  struct {
						Type   string `json:"type"`
						Reason string `json:"reason"`
						Cause  struct {
							Type   string `json:"type"`
							Reason string `json:"reason"`
						} `json:"caused_by"`
					} `json:"error"`
				} `json:"index"`
			} `json:"items"`
		}

		var (
			res        *esapi.Response
			err        error
			raw        map[string]interface{}
			blk        *bulkResponse
			numItems   int
			numErrors  int
			numIndexed int
		)

		start := time.Now().UTC()
		//import data
		res, err = esClient.Bulk(bytes.NewReader(task.buf.Bytes()), esClient.Bulk.WithIndex(task.indexName))
		if err != nil {
			log.Fatalf("Failure indexing %v: %s", task.indexName, err)
		}
		// If the whole request failed, print error and mark all documents as failed
		if res.IsError() {
			numErrors += numItems
			if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
				log.Fatalf("Failure to to parse response body: %s", err)
			} else {
				log.Printf("  Error: [%d] %s: %s",
					res.StatusCode,
					raw["error"].(map[string]interface{})["type"],
					raw["error"].(map[string]interface{})["reason"],
				)
			}
		} else { // A successful response might still contain errors for particular documents...
			if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
				log.Fatalf("Failure to to parse response body: %s", err)
			} else {
				for _, d := range blk.Items {
					// ... so for any HTTP status above 201 ...
					if d.Index.Status > 201 {
						numErrors++
						log.Printf("  Error: [%d]: %s: %s: %s: %s",
							d.Index.Status,
							d.Index.Error.Type,
							d.Index.Error.Reason,
							d.Index.Error.Cause.Type,
							d.Index.Error.Cause.Reason,
						)
					} else {
						numIndexed++
					}
				}
			}
		}

		// Close the response body, to prevent reaching the limit for goroutines or file handles
		res.Body.Close()
		// Reset the buffer and items counter
		task.buf.Reset()
		numItems = 0

		// Report the results: number of indexed docs, number of errors, duration, indexing rate
		log.Println(strings.Repeat("=", 80))
		dur := time.Since(start)
		if numErrors > 0 {
			log.Fatalf(
				"Indexed [%d] documents with [%d] errors in %s (%.0f docs/sec)",
				numIndexed,
				numErrors,
				dur.Truncate(time.Millisecond),
				1000.0/float64(dur/time.Millisecond)*float64(numIndexed),
			)
		} else {
			log.Printf(
				"Sucessfuly indexed [%d] documents in %s (%.0f docs/sec)",
				numIndexed,
				dur.Truncate(time.Millisecond),
				1000.0/float64(dur/time.Millisecond)*float64(numIndexed),
			)
		}
		task.Wg.Done()
	}
}

func es_begin(indexName string) {
	_, err := esClient.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("Cannot create index: %s", err)
	}
	item := make(map[string]map[string]string)
	item["index"] = make(map[string]string)
	item["index"]["number_of_replicas"] = "0"
	item["index"]["refresh_interval"] = "-1"
	setting, _ := json.Marshal(item)
	_, err = esClient.Indices.PutSettings(bytes.NewReader(setting), esClient.Indices.PutSettings.WithIndex(indexName))
	if err != nil {
		log.Fatalf("Cannot setting index: %s", err)
	}
}

func es_end(indexName string) {
	item := make(map[string]map[string]string)
	item["index"] = make(map[string]string)
	item["index"]["number_of_replicas"] = getMainConfValue(mainConf, "NUMBER_OF_REPLICAS", "") //"1"
	item["index"]["refresh_interval"] = getMainConfValue(mainConf, "REFRESH_INTERVAL", "")     //"10s"
	setting, _ := json.Marshal(item)
	_, err := esClient.Indices.PutSettings(bytes.NewReader(setting), esClient.Indices.PutSettings.WithIndex(indexName))
	if err != nil {
		log.Fatalf("Cannot setting index: %s", err)
	}
}
