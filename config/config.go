package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"io/ioutil"
)

const (
	dbConfigPath      = "./config/config_Files/db.yaml"
	logConfigPath     = "./config/config_Files/log.yaml"
	generalConfigPath = "./config/config_Files/general.yaml"
)

type Config struct {
}

// GetDB get the DbConfig Struct to maintain a connection
func (c Config) GetDB() (DbConfig, error) {
	var dbConfig DbConfig
	// check input correctness

	//fmt.Println(path)
	yamlFile, err := ioutil.ReadFile(dbConfigPath)
	if err != nil {
		fmt.Println(err.Error())
		return DbConfig{}, err
	}

	// Decode the read yaml File as targeted struct
	err = yaml.Unmarshal(yamlFile, &dbConfig)
	if err != nil {
		fmt.Println(err.Error())
		return DbConfig{}, err
	}
	return dbConfig, nil
}

// GetLogConfig get the LogConfig Struct to direct the path of Log File
func (c Config) GetLogConfig() (LogConfig, error) {
	var logConfig LogConfig
	// check input correctness

	yamlFile, err := ioutil.ReadFile(logConfigPath)
	if err != nil {
		fmt.Println(err.Error())
		return LogConfig{}, err
	}

	// Decode the read yaml File as targeted struct
	err = yaml.Unmarshal(yamlFile, &logConfig)
	if err != nil {
		fmt.Println(err.Error())
		return LogConfig{}, err
	}
	return logConfig, nil
}

// GetNodeNum get the node number of current server
func (c Config) GetNodeNum() (int, error) {
	var generalConfig GeneralConfig
	// check input correctness

	//fmt.Println(path)
	yamlFile, err := ioutil.ReadFile(generalConfigPath)
	if err != nil {
		fmt.Println(err.Error())
		return -1, err
	}

	// Decode the read yaml File as targeted struct
	err = yaml.Unmarshal(yamlFile, &generalConfig)
	if err != nil {
		fmt.Println(err.Error())
		return -1, err
	}
	return generalConfig.NodeNum, nil
}

// IsTerminal Check whether the program should output to terminal
func (c Config) IsTerminal() bool {
	var generalConfig GeneralConfig
	// check input correctness

	//fmt.Println(path)
	yamlFile, err := ioutil.ReadFile(generalConfigPath)
	if err != nil {
		fmt.Println(err.Error())
		return true
	}

	// Decode the read yaml File as targeted struct
	err = yaml.Unmarshal(yamlFile, &generalConfig)
	if err != nil {
		fmt.Println(err.Error())
		return true
	}
	return !generalConfig.HasOutPutFile
}

// GetOutputPath get the result Output Path
func (c Config) GetOutputPath() (string, error) {
	var generalConfig GeneralConfig
	// check input correctness

	//fmt.Println(path)
	yamlFile, err := ioutil.ReadFile(generalConfigPath)
	if err != nil {
		fmt.Println(err.Error())
		return "", err
	}

	// Decode the read yaml File as targeted struct
	err = yaml.Unmarshal(yamlFile, &generalConfig)
	if err != nil {
		fmt.Println(err.Error())
		return "", err
	}
	return generalConfig.OutPutPath, nil
}

// IsSQL Check whether the program should work in YSQL
func (c Config) IsSQL() bool {
	var generalConfig GeneralConfig
	// check input correctness

	//fmt.Println(path)
	yamlFile, err := ioutil.ReadFile(generalConfigPath)
	if err != nil {
		fmt.Println(err.Error())
		return true
	}

	// Decode the read yaml File as targeted struct
	err = yaml.Unmarshal(yamlFile, &generalConfig)
	if err != nil {
		fmt.Println(err.Error())
		return true
	}
	if generalConfig.IsSQL == 1 {
		return true
	}
	return false
}

// GetTotalNode get the Total Node num
func (c Config) GetTotalNode() int {
	var generalConfig GeneralConfig
	// check input correctness

	//fmt.Println(path)
	yamlFile, err := ioutil.ReadFile(generalConfigPath)
	if err != nil {
		fmt.Println(err.Error())
		return 20
	}

	// Decode the read yaml File as targeted struct
	err = yaml.Unmarshal(yamlFile, &generalConfig)
	if err != nil {
		fmt.Println(err.Error())
		return 20
	}
	return generalConfig.TotalNode
}
