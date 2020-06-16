package main

import (
	"github.com/Unknwon/goconfig"
	"log"
	"os"
)

func saveConf(cfg *goconfig.ConfigFile, confFile string) {
	// Save the configFile object to the file system, and the key order after saving is the same as that during reading
	err := goconfig.SaveConfigFile(cfg, confFile)
	if err != nil {
		log.Fatalf("Unable to save profile：%s", err)
	}
}

//get config file's string value (ex:conf.ini)
func getMainConfValue(cfg *goconfig.ConfigFile, name, section string) (value string) {
	value, err := cfg.GetValue(section, name)
	if err != nil {
		value = ""
		//log.Fatalf("Unable to get key value（%s）：%s", name, err)
	}
	return value
}

//get config file's int value (ex:conf.ini)
func getMainConfInt(cfg *goconfig.ConfigFile, name, section string) (value int) {
	value, err := cfg.Int(section, name)
	if err != nil {
		value = 0
		//log.Fatalf("Unable to get key value（%s）：%s", name, err)
	}
	return value
}

//get data file's string value (ex:data_conf.ini)
func getDataConfValue(name, section string) (value string) {
	value, err := dataConf.GetValue(section, name)
	if err != nil {
		value = ""
		//log.Fatalf("Unable to get key value（%s）：%s", name, err)
	}
	return value
}

//get data file's int value (ex:data_conf.ini)
func getDataConfInt(name, section string) (value int) {
	value, err := dataConf.Int(section, name)
	if err != nil {
		value = 0
		//log.Fatalf("Unable to get key value（%s）：%s", name, err)
	}
	return value
}

func setDataConfValue(name, value, section string) {
	// write key value
	dataConf.SetValue(section, name, value)
	saveConf(dataConf, configDataFile)
	dataConf.Reload()
}

func makeConf(confFile string) {
	//write file
	file, error := os.Create(confFile)
	if error != nil {
		log.Fatalf("Unable to create data file：%s", error)
	}
	data := "fullloadTimes=0\r\nincrementTimes=0\r\nlastPrimaryId=0\r\n"
	//Write string
	file.WriteString(data)
	file.Close()
}
