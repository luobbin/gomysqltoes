module mysqltoes

go 1.14

require (
	github.com/Unknwon/goconfig v0.0.0-20191126170842-860a72fb44fd
	github.com/elastic/go-elasticsearch/v7 v7.6.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/robfig/cron/v3 v3.0.1
	google.golang.org/cron v0.0.0-00010101000000-000000000000 // indirect
)

replace google.golang.org/cron => github.com/robfig/cron/v3 v3.0.0
