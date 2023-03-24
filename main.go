// Author: Group O
//

package main

import (
	"Distributed_DB_Project/config"
	"Distributed_DB_Project/cql_transaction"
	mylog "Distributed_DB_Project/log"
	"Distributed_DB_Project/performance_measurement"
	_ "Distributed_DB_Project/transaction"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/yugabyte/gocql"
	"log"
	"os"
	"time"
)

var DB *sql.DB
var session *gocql.Session

func main() {
	logFile := mylog.InitializeLog()
	defer logFile.Close()
	InitializeStdOut()
	InitializeDBConfig()
	defer DB.Close()
	//defer session.Close()
	//TestTransaction()
	Run(DB, session)
	performance_measurement.Throughput_result()
	performance_measurement.Compute_end_state_stats(DB)

}

func InitializeStdOut() {
	config := config.Config{}
	if !config.IsTerminal() {
		outputPath, err := config.GetOutputPath()
		if err != nil {
			mylog.Logger.ERRORf("GetStdOutPath Error [%s]", err.Error())
			return
		}
		stdoutPath, _ := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_SYNC|os.O_APPEND, 0755)
		os.Stdout = stdoutPath
		//os.Stderr = stdoutPath
	}
	return
}

func InitializeDBConfig() {
	config := config.Config{}
	DBConfig, err := config.GetDB()
	if err != nil {
		log.Println(err.Error())
	}
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		DBConfig.Host, DBConfig.Port, DBConfig.DbUser, DBConfig.DbPassword, DBConfig.DbName)

	if DBConfig.SslMode != "" {
		psqlInfo += fmt.Sprintf(" sslmode=%s", DBConfig.SslMode)

		if DBConfig.SslRootCert != "" {
			psqlInfo += fmt.Sprintf(" sslrootcert=%s", DBConfig.SslRootCert)
		}
	}

	DB, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		mylog.Logger.ERRORf("Open DB sql session Error [%s]", err.Error())
		log.Fatal(err.Error())
		return
	}
	checkIfError(err)

	cluster := gocql.NewCluster(DBConfig.Host)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "yugabyte",
		Password: "yugabyte",
	}
	// Use the same timeout as the Java driver.
	cluster.Timeout = 12 * time.Second
	//cluster.WriteCoalesceWaitTime = 10 * time.Second
	//cluster.ConnectTimeout = 12 * time.Second
	cluster.Keyspace = "schema_2"
	// Create the session.
	session, err = cluster.CreateSession()

	if err != nil {
		log.Fatal("Connect Error, Check your VPN " + err.Error())

		return
	}
}

func checkIfError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func TestTransaction() {
	fmt.Println("============================ BEFORE =================================")
	cql_transaction.CheckPayment(session, 1, 1, 1)

	cql_transaction.ExecPayment(session, 1, 1, 1, 1000)

	fmt.Println("============================ AFTER =================================")
	cql_transaction.CheckPayment(session, 1, 1, 1)
}
