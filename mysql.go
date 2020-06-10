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
	"time"
)

func get_mysql() *sql.DB {
	//sql_conn="root:123456@tcp(127.0.0.1:3306)/test?charset=utf8"
	db, err := sql.Open("mysql", sql_conn)
	checkErr(err)
	//db.SetConnMaxLifetime(time.Second * 5)
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(200)
	return db
}

//get total recode
func get_mysql_total(sql_str string) int {
	var total int
	db := get_mysql()
	sql_str = strings.ToLower(sql_str)
	sql_str = strings.Replace(sql_str, ":sql_last_value", "0", 1)
	reg := regexp.MustCompile(`^select (?s:(.*?)) from `)
	sql_str = reg.ReplaceAllString(sql_str, "select count(*) from ")
	//log.Printf("Get the total number of SQL is：%v \n", sql_str)
	err := db.QueryRow(sql_str).Scan(&total)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Get the total number of SQL as：%v,total number：%d \n", sql_str, total)
	err = db.Close()
	if err != nil {
		log.Println(err)
	}
	return total
}

//get begin ID
func get_mysql_firstid(sql_str string, field string, offset int) int {
	var id int
	db := get_mysql()
	sql_str = strings.ToLower(sql_str)
	sql_str = strings.Replace(sql_str, ":sql_last_value", "0", 1)
	reg := regexp.MustCompile(`^select (?s:(.*?)) from `)
	sql_str = reg.ReplaceAllString(sql_str, "select "+field+" from ")
	sql_str = sql_str + " ORDER BY " + field + " ASC LIMIT " + strconv.Itoa(offset) + ",1"
	log.Printf("Get the begin ID of SQL is：%v \n", sql_str)
	err := db.QueryRow(sql_str).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Get the begin ID of SQL as：%v,total number：%d \n", sql_str, id)
	err = db.Close()
	if err != nil {
		log.Println(err)
	}
	return id
}

//get last ID
func get_mysql_latsid(sql_str string, field string) int {
	var id int
	db := get_mysql()
	sql_str = strings.ToLower(sql_str)
	sql_str = strings.Replace(sql_str, ":sql_last_value", "0", 1)
	reg := regexp.MustCompile(`^select (?s:(.*?)) from `)
	sql_str = reg.ReplaceAllString(sql_str, "select MAX("+field+") AS id from ")
	//log.Printf("Get the end ID of SQL is：%v \n", sql_str)
	err := db.QueryRow(sql_str).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Get the end ID of SQL as：%v,total number：%d \n", sql_str, id)
	err = db.Close()
	if err != nil {
		log.Println(err)
	}
	return id
}

//Read the data from MySQL and turn it into JSON, and then use the cooperation process to concurrent to es library
func query_mysql_to_es_by_startid(sql_str string, id_start, id_end int) int {
	beginId := id_start //get start ID
	pageNum := 0
	var tasks [10]chan<- Chandata
	for i := 0; i < 10; i++ {
		tasks[i] = CreateChan()
	}
	for { //Process data on each page
		buf := new(bytes.Buffer)
		query_sql_str := strings.Replace(sql_str, ":sql_last_value", strconv.Itoa(beginId), 1) + " ORDER BY " + primary_key + " ASC LIMIT " + strconv.Itoa(page_size)
		log.Println("the query_sql_str is", query_sql_str)
		db := get_mysql()

		rows, err := db.Query(query_sql_str)
		checkErr(err)
		columns, _ := rows.Columns()
		scanArgs := make([]interface{}, len(columns))
		values := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		coltypes, _ := rows.ColumnTypes()

		for rows.Next() {
			//Save row data to record dictionary
			err = rows.Scan(scanArgs...)
			if err != nil {
				log.Printf("Sql row scan error %v \n", err)
			}

			record := make(map[string]interface{})
			for i, col := range values {
				if col != nil {
					value := string(col.([]byte))
					if columns[i] == strings.ReplaceAll(primary_key, "a.", "") {
						beginId, _ = strconv.Atoi(value)
					}
					switch coltypes[i].DatabaseTypeName() {
					case "VARCHAR", "CHAR", "TEXT":
						record[columns[i]] = value
					case "BOOL":
						record[columns[i]], _ = strconv.ParseBool(value)
					case "DOUBLE", "NUMERIC":
						record[columns[i]], _ = strconv.ParseFloat(value, 64)
					case "BIGINT", "INT", "TINYINT":
						record[columns[i]], _ = strconv.Atoi(value)
					case "DATETIME", "DATE":
						loc, _ := time.LoadLocation("Asia/Shanghai") //设置时区
						tt, _ := time.ParseInLocation("2006-01-02 15:04:05", value, loc)
						record[columns[i]] = tt
					default:
						record[columns[i]] = value
						log.Printf("sql field type name is %v\n", coltypes[i].DatabaseTypeName())
					}
				}
			}

			//beginId, _ = strconv.Atoi(record.(map[string]string)[primary_key])
			meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%v" } }%s`, record[index_docid_name], "\n"))
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
		tasks[pageNum%10] <- Chandata{indexName: index_name, buf: buf, Wg: &wg}
		//go save_es_data(buf, index_name, tasks,  wg)
		pageNum++
		err = rows.Close() // 记得关闭rows连接
		if err != nil {
			log.Println(err)
		}
		if beginId >= id_end {
			err = db.Close()
			if err != nil {
				log.Println(err)
			}
			break
		}
		//time.Sleep(time.Second*5)
	}
	log.Printf("The number of pages processed by the sub process from %d to %d is:%d\n", id_start, id_end, pageNum)
	wg.Wait()
	return pageNum
}

//Incremental data processing
func query_mysql_to_es_by_startid_nochan(sql_str string, id_start, id_end int) {
	beginId := id_start
	pageNum := 0
	var tasks [10]chan<- Chandata
	for i := 0; i < 10; i++ {
		tasks[i] = CreateChan()
	}
	//Process data on each page
	for {
		buf := new(bytes.Buffer)
		query_sql_str := strings.Replace(sql_str, ":sql_last_value", strconv.Itoa(beginId), 1) + " ORDER BY " + primary_key + " ASC LIMIT " + strconv.Itoa(page_size)
		db := get_mysql()

		rows, err := db.Query(query_sql_str) //"SELECT * FROM user"
		checkErr(err)
		//Construct two arrays of scanargs and values. Each value of scanargs points to the address of the corresponding value of values
		columns, _ := rows.Columns()
		scanArgs := make([]interface{}, len(columns))
		values := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		coltypes, _ := rows.ColumnTypes()

		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				log.Printf("Sql row scan error %v \n", err)
			}
			record := make(map[string]interface{})
			for i, col := range values {
				if col != nil {
					value := string(col.([]byte))
					if columns[i] == strings.ReplaceAll(primary_key, "a.", "") {
						beginId, _ = strconv.Atoi(value)
					}
					switch coltypes[i].DatabaseTypeName() {
					case "VARCHAR", "CHAR", "TEXT":
						record[columns[i]] = value
					case "BOOL":
						record[columns[i]], _ = strconv.ParseBool(value)
					case "DOUBLE", "NUMERIC":
						record[columns[i]], _ = strconv.ParseFloat(value, 64)
					case "BIGINT", "INT", "TINYINT":
						record[columns[i]], _ = strconv.Atoi(value)
					case "DATETIME", "DATE":
						loc, _ := time.LoadLocation("Asia/Shanghai") //设置时区
						tt, _ := time.ParseInLocation("2006-01-02 15:04:05", value, loc)
						record[columns[i]] = tt
					default:
						record[columns[i]] = value
						log.Printf("sql field type name is %v\n", coltypes[i].DatabaseTypeName())
					}
				}
			}
			meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%v" } }%s`, record[index_docid_name], "\n"))
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
		tasks[pageNum%10] <- Chandata{indexName: index_name, buf: buf, Wg: &wg}
		//go save_es_data(buf, index_name, &wg)
		wg.Wait()
		pageNum++
		err = rows.Close() // 记得关闭rows连接
		if err != nil {
			log.Println(err)
		}
		if beginId >= id_end {
			err = db.Close()
			if err != nil {
				log.Println(err)
			}
			break
		}
		//time.Sleep(time.Second*5)
	}
	//log.Printf("The number of pages processed by incremental task from %d to %d is: %d\n", id_start, id_end,pageNum)
}

func CreateChan() chan<- Chandata {
	task := make(chan Chandata)
	go save_es_data(task)
	return task
}
