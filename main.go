package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/Unknwon/goconfig"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/robfig/cron/v3"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	primary_key      string
	index_name       string
	index_docid_name string
	index_time_fomat int
	page_size        int
	sql_str          string
	sql_conn         string
	timer_increment  string
	timer_fullload   string
	mutex            sync.Mutex
	esClient         *elasticsearch.Client
	dataConf         *goconfig.ConfigFile
	mainConf         *goconfig.ConfigFile
	configDataFile   string
	wg               sync.WaitGroup
	err              error
	configFile       = flag.String("configFile", "etc/conf.ini", "Set profile file：")
	manual           = flag.String("manual", "0", "manual control fullload：")
	logon            = flag.String("logon", "0", "log switch control fullload：")
)

type Chandata struct {
	indexName string
	buf       *bytes.Buffer
	Wg        *sync.WaitGroup
}

//command：go run mysqltoes -configFile etc/user_conf.ini
func init() {
	flag.Parse()
	if *logon != "0" {
		log_file := strings.Replace(*configFile, "etc/", "etc/log/", 1) + ".log"
		logFile, err := os.OpenFile(log_file, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("Log file open Error: %v", err)
		}
		log.SetOutput(logFile)
	}
}

func main() {
	//check the config file exits
	mainConf, err = goconfig.LoadConfigFile(*configFile)
	if err != nil {
		log.Fatalf("Unable to load profile：%s", err)
	}
	//default settings
	primary_key = getMainConfValue(mainConf, "PRIMARY_KEY", "")
	index_name = getMainConfValue(mainConf, "INDEX_NAME", "")
	index_docid_name = getMainConfValue(mainConf, "INDEX_DOCID_NAME", "")
	index_time_fomat = getMainConfInt(mainConf, "INDEX_TIME_FORMAT", "")
	sql_str = getMainConfValue(mainConf, "SQL_STR", "")
	page_size = getMainConfInt(mainConf, "PAGE_SIZE", "")
	timer_increment = getMainConfValue(mainConf, "TIMER_INCREMENT", "")
	timer_fullload = getMainConfValue(mainConf, "TIMER_FULLLOAD", "")
	//mysql settings
	mysql_conf, _ := mainConf.GetSection("MYSQL")
	sql_conn = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", mysql_conf["DB_USERNAME"], mysql_conf["DB_PASSWORD"], mysql_conf["DB_DSN"], mysql_conf["DB_DATABASE"])
	//es settings
	es_conf, _ := mainConf.GetSection("ELASTIC")
	//log.Println(es_conf["ES_ADDRS"])
	addrs := strings.Split(es_conf["ES_ADDRS"], ",")
	//log.Println(addrs)
	esClient = get_es(addrs, es_conf["ES_USERNAME"], es_conf["ES_PASSWORD"])
	//check the data file exits
	configDataFile = strings.Replace(*configFile, "etc/", "etc/record/data_", 1)
	dataConf, err = goconfig.LoadConfigFile(configDataFile)
	if err != nil {
		makeConf(configDataFile) //Create data profile
		dataConf, _ = goconfig.LoadConfigFile(configDataFile)
	}

	if *manual == "0" {
		run()
	} else {
		task_fullload()
	}

	//task_fullload()

}

func run() {

	c := cron.New(cron.WithSeconds())

	//"0/5 * * * * *" Every 5S
	c.AddFunc("CRON_TZ=Asia/Shanghai "+timer_increment, task_increment)

	//"0 0 */1 * * *"	Hourly execution
	c.AddFunc("CRON_TZ=Asia/Shanghai "+timer_fullload, task_fullload)
	c.Start()

	select {}
}

/*
测试函数异常死机恢复（panic异常）
func TestFuncPanicRecovery(t *testing.T) {
	var buf syncWriter
	cron := New(WithParser(secondParser),
		WithChain(Recover(newBufLogger(&buf))))
	cron.Start()
	defer cron.Stop()
	cron.AddFunc("* * * * * ?", func() {
		panic("YOLO")
	})

	select {
	case <-time.After(OneSecond):
		if !strings.Contains(buf.String(), "YOLO") {
			t.Error("expected a panic to be logged, got none")
		}
		return
	}
}*/

//Incremental timer
func task_increment() {
	bT := time.Now()
	//Check if the last ID needs to be processed
	lastPrimaryId := getDataConfInt("lastPrimaryId", "")

	if lastPrimaryId > 0 {
		lastPrimaryIdNow := get_mysql_latsid(sql_str, primary_key)
		//log.Printf("1: %v 2: %v\n", lastPrimaryIdNow,lastPrimaryId)
		if lastPrimaryIdNow > lastPrimaryId {
			mutex.Lock()
			//Set the number of incremental backups
			incrementTimes := getDataConfInt("incrementTimes", "") + 1
			setDataConfValue("incrementTimes", strconv.Itoa(incrementTimes), "")

			query_mysql_to_es_by_startid_nochan(sql_str, lastPrimaryId, lastPrimaryIdNow)

			//Last value added after processing record
			setDataConfValue("lastPrimaryId", strconv.Itoa(lastPrimaryIdNow), "")

			mutex.Unlock()

			eT := time.Since(bT)
			log.Printf("Index: %v incrementally processed from %v, time consuming(s): %v\n", index_name, lastPrimaryId, eT)
		}

	}
	log.Printf("Index: %v no incremental tasks to process\n", index_name)

}

//Fullload timer
func task_fullload() {
	bT := time.Now()
	numOfConcurrency := runtime.NumCPU()
	log.Printf("CPU Tota：%d\n", numOfConcurrency)
	runtime.GOMAXPROCS(numOfConcurrency)

	//Initialize es index and other data
	es_begin(index_name)

	fullloadTimes := getDataConfInt("fullloadTimes", "") + 1
	id_start := 0
	id_end := get_mysql_latsid(sql_str, primary_key)
	setDataConfValue("lastPrimaryId", strconv.Itoa(id_end), "")
	sum := query_mysql_to_es_by_startid(sql_str, id_start, id_end)

	//save success times
	setDataConfValue("fullloadTimes", strconv.Itoa(fullloadTimes), "")
	//Restore index configuration at end
	es_end(index_name)

	eT := time.Since(bT)
	log.Printf("Index: %v total pages processed, total pages processed: %d, execution time (s): %v\n", index_name, sum, eT)
}

func checkErr(err error) {
	if err != nil {
		log.Fatalf("Data Error: %s", err)
	}
}
