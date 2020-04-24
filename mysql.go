package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

func get_mysql() *sql.DB {
	//sql_conn="root:123456@tcp(127.0.0.1:3306)/test?charset=utf8"
	db, err := sql.Open("mysql", sql_conn)
	checkErr(err)
	return db
}

//get total recode
func get_mysql_total(sql_str string) int {
	var total int
	db := get_mysql()
	defer db.Close()

	sql_str = strings.ToLower(sql_str)
	sql_str = strings.Replace(sql_str, ":sql_last_value", "0", 1)
	reg := regexp.MustCompile(`^select(?s:(.*?))from`)
	sql_str = reg.ReplaceAllString(sql_str, "select count(*) from")

	err := db.QueryRow(sql_str).Scan(&total)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Get the total number of SQL as：%v,total number：%d \n", sql_str, total)
	return total
}

//get begin ID
func get_mysql_firstid(sql_str string, field string, offset int) int {
	var id int
	db := get_mysql()
	defer db.Close()
	sql_str = strings.ToLower(sql_str)
	sql_str = strings.Replace(sql_str, ":sql_last_value", "0", 1)
	reg := regexp.MustCompile(`^select(?s:(.*?))from`)
	sql_str = reg.ReplaceAllString(sql_str, "select "+field+" from")
	sql_str = sql_str + " ORDER BY " + field + " ASC LIMIT 1 OFFSET " + strconv.Itoa(offset)

	err := db.QueryRow(sql_str).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	return id
}

//get last ID
func get_mysql_latsid(sql_str string, field string) int {
	var id int
	db := get_mysql()
	defer db.Close()
	sql_str = strings.ToLower(sql_str)
	sql_str = strings.Replace(sql_str, ":sql_last_value", "0", 1)
	reg := regexp.MustCompile(`^select(?s:(.*?))from`)
	sql_str = reg.ReplaceAllString(sql_str, "select MAX("+field+") AS id from")
	err := db.QueryRow(sql_str).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	return id
}

//Read the data from MySQL and turn it into JSON, and then use the cooperation process to concurrent to es library
func query_mysql_to_es_by_startid(sql_str string, id_start, id_end int, job chan int, wg *sync.WaitGroup) {
	defer close(job)
	defer wg.Done()
	beginId := id_start //get start ID
	pageNum := 0

	for { //Process data on each page
		buf := new(bytes.Buffer)
		query_sql_str := strings.Replace(sql_str, ":sql_last_value", strconv.Itoa(beginId), 1) + " ORDER BY " + primary_key + " ASC LIMIT " + strconv.Itoa(page_size)
		log.Println("the query_sql_str is", query_sql_str)
		db := get_mysql()
		defer db.Close()

		rows, err := db.Query(query_sql_str)
		checkErr(err)
		columns, _ := rows.Columns()
		scanArgs := make([]interface{}, len(columns))
		values := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		for rows.Next() {
			//Save row data to record dictionary
			err = rows.Scan(scanArgs...)
			record := make(map[string]string)
			for i, col := range values {
				if col != nil {
					record[columns[i]] = string(col.([]byte))
				}
			}
			beginId, _ = strconv.Atoi(record[primary_key])
			meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d" } }%s`, beginId, "\n"))
			json_data, err := json.Marshal(record)
			if err != nil {
				log.Fatalf("Cannot encode %v %d: %s", index_name, beginId, err)
			}
			// Append newline to the data payload
			json_data = append(json_data, "\n"...)
			// Append payloads to the buffer (ignoring write errors)
			buf.Grow(len(meta) + len(json_data))
			buf.Write(meta)
			buf.Write(json_data)
		}
		log.Println("the last ID is", beginId)
		wg.Add(1)
		go save_es_data(buf, index_name, wg)
		pageNum++
		if beginId >= id_end {
			break
		}
		time.Sleep(time.Second)
	}
	log.Printf("The number of pages processed by the sub process from %d to %d is:%d\n", id_start, id_end, pageNum)
	job <- pageNum
}

//Incremental data processing
func query_mysql_to_es_by_startid_nochan(sql_str string, id_start, id_end int) {
	beginId := id_start
	pageNum := 0
	//Process data on each page
	for {
		buf := new(bytes.Buffer)
		query_sql_str := strings.Replace(sql_str, ":sql_last_value", strconv.Itoa(beginId), 1) + " ORDER BY " + primary_key + " ASC LIMIT " + strconv.Itoa(page_size)
		db := get_mysql()
		defer db.Close()

		rows, err := db.Query(query_sql_str) //"SELECT * FROM user"
		checkErr(err)
		//Construct two arrays of scanargs and values. Each value of scanargs points to the address of the corresponding value of values
		columns, _ := rows.Columns()
		scanArgs := make([]interface{}, len(columns))
		values := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		for rows.Next() {
			err = rows.Scan(scanArgs...)
			record := make(map[string]string)
			for i, col := range values {
				if col != nil {
					record[columns[i]] = string(col.([]byte))
				}
			}
			beginId, _ = strconv.Atoi(record[primary_key])
			meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d" } }%s`, beginId, "\n"))
			json_data, err := json.Marshal(record)
			if err != nil {
				log.Fatalf("Cannot encode recode %d: %s", beginId, err)
			}
			// Append newline to the data payload
			json_data = append(json_data, "\n"...)
			// Append payloads to the buffer (ignoring write errors)
			buf.Grow(len(meta) + len(json_data))
			buf.Write(meta)
			buf.Write(json_data)

		}
		//log.Println("the last ID is",beginId)
		wg.Add(1)
		go save_es_data(buf, index_name, &wg)
		wg.Wait()
		pageNum++
		if beginId >= id_end {
			break
		}
	}
	//log.Printf("The number of pages processed by incremental task from %d to %d is: %d\n", id_start, id_end,pageNum)
}
