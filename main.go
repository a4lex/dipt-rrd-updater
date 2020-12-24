package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	h "github.com/a4lex/go-helpers"
	rrd "github.com/ziutek/rrd"
)

const (
	sqlGetQuery = `SELECT query, rate, type, min, max, threshold, CONCAT(shared, '/', name) AS path FROM snmp_templates WHERE source='mysql'`
)

var (
	configPath = flag.String("configs-file", "config.yml", "Configs path")
	logPath    = flag.String("log-file", fmt.Sprintf("%s.log", os.Args[0]), "Log path")
	logVerbose = flag.Int("log-level", 63, "Log verbose [ 1 - Fatal, 2 - ERROR, 4 - INFO, 8 - MYSQL, 16 - FUNC, 32 - DEBUG ]")

	delay        = flag.Int("delay", 0, "Delay before run program")
	maxPerUpdate = flag.Int("max-values", 10000, "Count of values for select from DB to update")
	dirRRD       = flag.String("dir-rrd", "/tmp/rrd", "Path to store RRD files")

	l      h.MyLogger
	mysqli h.MySQL
	cfg    h.Config
)

func main() {
	flag.Parse()

	//
	// Init logs
	//

	f, err := os.OpenFile(*logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(fmt.Sprintf("error opening file: %v", err))
	}
	defer f.Close()

	l = h.InitLog(f, *logVerbose)

	//
	// Sleep before start - another programs should do own tasks
	//

	time.Sleep(time.Duration(*delay) * time.Second)
	l.Printf(h.FUNC, "START")

	//
	// Load config vars
	//

	if err = h.LoadConfig(*configPath, &cfg); err != nil {
		panic(fmt.Sprintf("can not init config: %v", err))
	}

	//
	// Init MySQL connection
	//

	mysqli, err = h.DBConnect(&l, cfg.Database.Host, cfg.Database.User, cfg.Database.Pass, cfg.Database.Database, cfg.Database.DSN, cfg.Database.MaxIdle, cfg.Database.MaxOpen)
	if err != nil {
		panic(fmt.Sprintf("failed connect to DB: %v", err))
	}

	//
	// Main Loop
	//

	timeUpdRRD := time.Now()
	listSNMPTemplates := mysqli.DBSelectList(sqlGetQuery)
	for _, template := range listSNMPTemplates {

		// create dir for rrd files, for current template
		rrdDirTemplate := fmt.Sprintf("%s/%s", *dirRRD, template["path"])
		if _, err := os.Stat(rrdDirTemplate); os.IsNotExist(err) {
			os.MkdirAll(rrdDirTemplate, os.ModePerm)
			l.Printf(h.INFO, "Create dir for RRD files: %s", rrdDirTemplate)
		}

		min, _ := strconv.Atoi(template["min"])
		max, _ := strconv.Atoi(template["max"])

		// loop by values of template
		// select not all values, pick just part
		// of it every iteration of the loop
		offset := 0
		for {
			listVals := mysqli.DBSelectList(fmt.Sprintf("%s LIMIT %d, %d", template["query"], offset, *maxPerUpdate))

			for _, val := range listVals {

				// store value in rrd db or create it
				fileRRD := fmt.Sprintf("%s/%s", rrdDirTemplate, val["file"])
				valRRD, _ := strconv.Atoi(val["val"])

				if err := RRDUpdate(fileRRD, timeUpdRRD, valRRD); err != nil {
					if _, err := os.Stat(fileRRD); os.IsExist(err) {
						continue
					}
					if _, err := RRDCreate(fileRRD, template["type"], min, max); err != nil {
						l.Printf(h.ERROR, "Can not create rrddb: %s - %s", val["path"], err)
					}
				}
			}

			if len(listVals) <= *maxPerUpdate {
				break
			}
			offset += *maxPerUpdate
		}

	}

	l.Printf(h.FUNC, "END")
}

//
// RRDCreate - create file of RRD DB
//
func RRDCreate(dbfile, counterType string, min, max int) (*rrd.Creator, error) {

	c := rrd.NewCreator(dbfile, time.Now(), 300)
	c.DS("val", counterType, 600, min, max)
	c.RRA("AVERAGE", 0.5, 1, 288)
	c.RRA("LAST", 0.5, 1, 288)
	c.RRA("MIN", 0.5, 1, 288)
	c.RRA("MAX", 0.5, 1, 288)
	c.RRA("AVERAGE", 0.5, 7, 288)
	c.RRA("LAST", 0.5, 7, 288)
	c.RRA("MIN", 0.5, 7, 288)
	c.RRA("MAX", 0.5, 7, 288)
	c.RRA("AVERAGE", 0.5, 30, 288)
	c.RRA("LAST", 0.5, 30, 288)
	c.RRA("MIN", 0.5, 30, 288)
	c.RRA("MAX", 0.5, 30, 288)
	c.RRA("AVERAGE", 0.5, 365, 288)
	c.RRA("LAST", 0.5, 365, 288)
	c.RRA("MIN", 0.5, 365, 288)
	c.RRA("MAX", 0.5, 365, 288)

	if err := c.Create(true); err != nil {
		l.Printf(h.ERROR, "Can not create RRD DB: %s, counterType: %s, min: %d, max: %d", dbfile, counterType, min, max)
		return nil, err
	} else {
		l.Printf(h.DEBUG, "Create RRD DB: %s, counterType: %s, min: %d, max: %d", dbfile, counterType, min, max)
		return c, nil
	}
}

//
// RRDUpdate - update RRD DB file with given value and time
//
func RRDUpdate(dbfile string, time time.Time, val int) error {
	u := rrd.NewUpdater(dbfile)
	if err := u.Update(time, val); err != nil {
		l.Printf(h.DEBUG, "Update is failed RRD DB: %s, time: %s, val: %d, error: %s", dbfile, time.Format("2006-01-02 15:04:05"), val, err)
		return err
	}
	l.Printf(h.DEBUG, "Update RRD DB: %s, time: %s, val: %d", dbfile, time.Format("2006-01-02 15:04:05"), val)
	return nil
}
