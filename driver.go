package main

import (
	"Distributed_DB_Project/config"
	"Distributed_DB_Project/cql_transaction"
	mylog "Distributed_DB_Project/log"
	"Distributed_DB_Project/transaction"
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/yugabyte/gocql"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	LOCAL_XACT_PATH = "./xact_files/%d.txt"
)

var (
	transactionTimes = make([]float64, 0)
	clientNumber     = 0
	n                = make([]int, 20)
)

func Run(db *sql.DB, session *gocql.Session) {
	var config config.Config
	dbIdx, err := config.GetNodeNum()
	TotalNum := config.GetTotalNode()
	log.Printf("Current Node Number is %d", dbIdx)
	n[0] = 8132
	n[1] = 25239
	n[2] = 16038
	n[3] = 10224
	n[4] = 13082
	n[5] = 5253
	n[6] = 13121
	n[7] = 11070
	n[8] = 6605
	n[9] = 10862
	n[10] = 42208
	n[11] = 46437
	n[12] = 8762
	n[13] = 14307
	n[14] = 7085
	n[15] = 11643
	n[16] = 70682
	n[17] = 67372
	n[18] = 11034
	n[19] = 33805

	if err != nil {
		log.Fatal(err)
	}
	mylog.Logger.INFO("==========================Start Benchmarking==========================")
	for i := 0; i < TotalNum; i++ {
		if i%5 == dbIdx {
			path := fmt.Sprintf(LOCAL_XACT_PATH, i)
			log.Printf("Parsing Xact file [%s]\n", path)
			mylog.Logger.INFOf("Parsing Xact file [%s]", path)
			clientNumber = i
			drive(db, session, path, n[i])
		}
	}
	//drive(db, session, "./xact_files/0.txt")
	mylog.Logger.INFO("==========================Finish Benchmarking==========================")
}

func drive(db *sql.DB, session *gocql.Session, filename string, n int) {
	now := 0
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	// for each line, call the corresponding transaction func
	scanner := bufio.NewScanner(file)
	var config config.Config
	for scanner.Scan() {
		line := strings.Split(scanner.Text(), ",")
		var params []interface{}
		var paramsTN [][]int
		paramsTN = make([][]int, 3)
		if line[0] == "N" {
			sz, err := strconv.Atoi(line[4])
			if err != nil {
				log.Fatal(err)
			}
			for i := 0; i < sz; i++ {
				scanner.Scan()
				nLine := strings.Split(scanner.Text(), ",")
				for i, v := range nLine {
					v, err := strconv.Atoi(v)
					if err != nil {
						log.Fatal(err)
					}
					paramsTN[i] = append(paramsTN[i], v)
				}
			}
		}
		for i, value := range line[1:] {
			var v interface{}
			var err error
			if i == 3 && line[0] == "P" {
				v, err = strconv.ParseFloat(value, 32)
			} else {
				v, err = strconv.Atoi(value)
			}

			if err != nil {
				mylog.Logger.ERRORf("Preparing SQL, %s  i=%d Error [%s]", line[0], i, err.Error())
				log.Fatal(err)
				return
			}
			params = append(params, v)
		}

		if config.IsSQL() {
			log.Printf("Running SQL, %s, now serving %d/%d,[%2.2f%%]", line[0], now, n, float32(now)/float32(n)*100)
			//mylog.Logger.DEBUGf("Running SQL, %s  %d  %d", line[0], params[0], params[1])
			processCommandSQL(db, line[0], params, paramsTN)
		} else {
			log.Printf("Running CQL, %s, now serving %d/%d,[%2.2f%%]", line[0], now, n, float32(now)/float32(n)*100)
			mylog.Logger.DEBUGf("Running CQL, %s  %d  %d", line[0])
			processCommandCQL(session, line[0], params, paramsTN)
		}
		now++
	}
	fmt.Println("Output performance matrix")
	var length = len(transactionTimes)
	var numberOfTransactionsExecuted = float64(len(transactionTimes))
	var totalTransactionExecutionTime = sum(transactionTimes)
	var transactionThroughput = numberOfTransactionsExecuted / totalTransactionExecutionTime
	var averageTransactionLatency = 1 / transactionThroughput * 1000
	//fmt.Printf("avgTransactionLatency is %f\n", averageTransactionLatency)
	sort.Float64s(transactionTimes)
	log.Printf("%v", transactionTimes)
	var medianTransactionLatency = transactionTimes[(len(transactionTimes)/2)] * 1000
	var ninetyFifthPercentileTransactionLatency = transactionTimes[length*95/100] * 1000
	var ninetyNinthPercentileTransactionLatency = transactionTimes[length*99/100] * 1000
	// Write to csv
	csvFile, err := os.OpenFile("./results/clients.csv", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}
	var clientsResult [][]string
	var temp []string
	csvwriter := csv.NewWriter(csvFile)

	temp = []string{
		fmt.Sprintf("%v", clientNumber),
		fmt.Sprintf("%f", numberOfTransactionsExecuted),
		fmt.Sprintf("%f", totalTransactionExecutionTime),
		fmt.Sprintf("%f", transactionThroughput),
		fmt.Sprintf("%f", averageTransactionLatency),
		fmt.Sprintf("%f", medianTransactionLatency),
		fmt.Sprintf("%f", ninetyFifthPercentileTransactionLatency),
		fmt.Sprintf("%f", ninetyNinthPercentileTransactionLatency)}
	clientsResult = append(clientsResult, temp)

	for _, empRow := range clientsResult {
		fmt.Print(empRow)
		_ = csvwriter.Write(empRow)
	}
	transactionTimes = make([]float64, 0)
	csvwriter.Flush()
	csvFile.Close()
}

func processCommandSQL(db *sql.DB, command string, params []interface{}, paramsTN [][]int) {
	//log.Println("Executing transactions, measuring start time")
	start := time.Now()
	log.Printf("Time now is %v", start)
	switch command {
	case "N":
		mylog.Logger.INFOf("Running SQL, %s  %d %d %d %d",
			command, params[0].(int), params[1].(int), params[2].(int), params[3].(int))

		transaction.ExecNewOrder(db, params[1].(int), params[2].(int), params[0].(int), params[3].(int), paramsTN[0], paramsTN[1], paramsTN[2])
		break
	case "P":
		mylog.Logger.INFOf("Running SQL, %s  %d %d %d %f",
			command, params[0].(int), params[1].(int), params[2].(int), params[3].(float64))

		transaction.ExecPayment(db, params[0].(int), params[1].(int), params[2].(int), params[3].(float64))
		break
	case "D":
		mylog.Logger.INFOf("Running SQL, %s  %d %d", command, params[0].(int), params[1].(int))
		//mylog.Logger.DEBUGf("Skip")
		transaction.ExecDelivery(db, params[0].(int), params[1].(int))
		break
	case "O":
		mylog.Logger.INFOf("Running SQL, %s  %d %d %d",
			command, params[0].(int), params[1].(int), params[2].(int))

		transaction.ExecOrderStatus(db, params[0].(int), params[1].(int), params[2].(int))
		break
	case "S":
		mylog.Logger.INFOf("Running SQL, %s  %d %d %d %d",
			command, params[0].(int), params[1].(int), params[2].(int), params[3].(int))

		transaction.ExecStockLevel(db, params[0].(int), params[1].(int), params[2].(int), params[3].(int))
		break
	case "I":
		mylog.Logger.INFOf("Running SQL, %s  %d %d %d",
			command, params[0].(int), params[1].(int), params[2].(int))

		transaction.ExcPopularItem(db, params[0].(int), params[1].(int), params[2].(int))
		break
	case "T":
		mylog.Logger.INFOf("Running SQL, %s", command)
		transaction.ExcTopBalance(db)
		break
	case "R":
		mylog.Logger.INFOf("Running SQL, %s  %d %d %d",
			command, params[0].(int), params[1].(int), params[2].(int))

		transaction.RelatedCustomer(db, params[0].(int), params[1].(int), params[2].(int))
		break
	default:
		errorString := fmt.Sprintf("No such type of transaction! %s", command)
		panic(errorString)
	}

	executionTime := -1 * (start.Sub(time.Now()).Seconds())
	log.Printf("execution time is %v\n", executionTime)
	transactionTimes = append(transactionTimes, executionTime)
}

func sum(array []float64) float64 {
	result := 0.00
	for _, v := range array {
		result += v
	}
	return result
}

func processCommandCQL(db *gocql.Session, command string, params []interface{}, paramsTN [][]int) {
	log.Println("Executing transactions, measuring start time")
	start := time.Now()
	log.Printf("Time now is %v", start)
	switch command {
	case "N":
		mylog.Logger.INFOf("Running SQL, %s  %d %d %d %d",
			command, params[0].(int), params[1].(int), params[2].(int), params[3].(int))

		cql_transaction.ExecNewOrder(db, params[1].(int), params[2].(int), params[0].(int), params[3].(int), paramsTN[0], paramsTN[1], paramsTN[2])
		break
	case "P":
		mylog.Logger.INFOf("Running SQL, %s  %d %d %d %f",
			command, params[0].(int), params[1].(int), params[2].(int), params[3].(float64))

		cql_transaction.ExecPayment(db, params[0].(int), params[1].(int), params[2].(int), params[3].(float64))
		break
	case "D":
		mylog.Logger.INFOf("Running SQL, %s  %d %d", command, params[0].(int), params[1].(int))

		cql_transaction.ExecDelivery(db, params[0].(int), params[1].(int))
		break
	case "O":
		mylog.Logger.INFOf("Running SQL, %s  %d %d %d",
			command, params[0].(int), params[1].(int), params[2].(int))

		cql_transaction.ExecOrderStatus(db, params[0].(int), params[1].(int), params[2].(int))
		break
	case "S":
		mylog.Logger.INFOf("Running SQL, %s  %d %d %d %d",
			command, params[0].(int), params[1].(int), params[2].(int), params[3].(int))

		cql_transaction.ExecStockLevel(db, params[0].(int), params[1].(int), float32(params[2].(int)), params[3].(int))
		break
	case "I":
		mylog.Logger.INFOf("Running SQL, %s  %d %d %d",
			command, params[0].(int), params[1].(int), params[2].(int))

		cql_transaction.ExcPopularItem(db, params[0].(int), params[1].(int), params[2].(int))
		break
	case "T":
		mylog.Logger.INFOf("Running SQL, %s", command)
		cql_transaction.ExcTopBalance(db)
		break
	case "R":
		mylog.Logger.INFOf("Running SQL, %s  %d %d %d",
			command, params[0].(int), params[1].(int), params[2].(int))

		cql_transaction.RelatedCustomer(db, params[0].(int), params[1].(int), params[2].(int))
		break
	default:
		errorString := fmt.Sprintf("No such type of transaction! %s", command)
		panic(errorString)
	}
	executionTime := -1 * (start.Sub(time.Now()).Seconds())
	log.Printf("execution time is %v\n", executionTime)
	transactionTimes = append(transactionTimes, executionTime)
}
