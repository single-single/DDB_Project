package performance_measurement

import (
	mylog "Distributed_DB_Project/log"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
)

func Compute_end_state_stats(db *sql.DB) {
	var stats [][]string
	var temp []string
	//sum(W_YTD) from warehouse
	sumWarehouseResult, err := db.Query(`SELECT sum(W_YTD) FROM warehouse`)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
	}
	var sumYTDResult float64
	if sumWarehouseResult.Next() {
		err = sumWarehouseResult.Scan(&sumYTDResult)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			fmt.Println(err.Error())
			return
		}
		temp = []string{fmt.Sprintf("%f", sumYTDResult)}
		stats = append(stats, temp)
	}
	sumWarehouseResult.Close()
	//sum(D_YTD), sum(D_NEXT_O_ID) from District
	sumDistrictResult, err := db.Query(`SELECT sum(D_YTD), sum(D_NEXT_O_ID) FROM district`)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
	}
	var sumDYTDResult float64
	var sumDNEXTOIDResult int

	if sumDistrictResult.Next() {
		err = sumDistrictResult.Scan(&sumDYTDResult, &sumDNEXTOIDResult)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			fmt.Println(err.Error())
			return
		}
		temp = []string{fmt.Sprintf("%f", sumDYTDResult)}
		stats = append(stats, temp)
		temp = []string{fmt.Sprintf("%v", sumDNEXTOIDResult)}
		stats = append(stats, temp)
	}
	sumDistrictResult.Close()

	//sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from customer
	sumCustomerResult, err := db.Query(`SELECT sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) FROM customer`)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
	}
	var sumCBALANCEResult float64
	var sumCYTDPAYMENTResult float64
	var sumCPAYMENTCNTResult int
	var sumCDELIVERYCNTResult int

	if sumCustomerResult.Next() {
		err = sumCustomerResult.Scan(&sumCBALANCEResult, &sumCYTDPAYMENTResult, &sumCPAYMENTCNTResult, &sumCDELIVERYCNTResult)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			fmt.Println(err.Error())
			return
		}

		temp = []string{fmt.Sprintf("%f", sumCBALANCEResult)}
		stats = append(stats, temp)
		temp = []string{fmt.Sprintf("%f", sumCYTDPAYMENTResult)}
		stats = append(stats, temp)
		temp = []string{fmt.Sprintf("%v", sumCPAYMENTCNTResult)}
		stats = append(stats, temp)
		temp = []string{fmt.Sprintf("%v", sumCDELIVERYCNTResult)}
		stats = append(stats, temp)
	}
	sumCustomerResult.Close()
	//max(O_ID), sum(O_OL_CNT) from Order
	sumOrdersResult, err := db.Query(`SELECT max(O_ID), sum(O_OL_CNT) FROM orders`)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
	}
	var sumOIDResult int
	var sumOOLCNTResult float64
	if sumOrdersResult.Next() {
		err = sumOrdersResult.Scan(&sumOIDResult, &sumOOLCNTResult)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			fmt.Println(err.Error())
			return
		}
		temp = []string{fmt.Sprintf("%v", sumOIDResult)}
		stats = append(stats, temp)
		temp = []string{fmt.Sprintf("%f", sumOOLCNTResult)}
		stats = append(stats, temp)
	}
	sumOrdersResult.Close()
	//sum(OL_AMOUNT), sum(OL_QUANTITY) from orderLine
	sumOrderLineResult, err := db.Query(`SELECT sum(OL_AMOUNT), sum(OL_QUANTITY) FROM orderLine`)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
	}
	var sumOLAMOUNTResult float64
	var sumOLQUANTITYResult int
	if sumOrderLineResult.Next() {
		err = sumOrderLineResult.Scan(&sumOLAMOUNTResult, &sumOLQUANTITYResult)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			fmt.Println(err.Error())
			return
		}
		temp = []string{fmt.Sprintf("%f", sumOLAMOUNTResult)}
		stats = append(stats, temp)
		temp = []string{fmt.Sprintf("%f", sumOLQUANTITYResult)}
		stats = append(stats, temp)
	}
	sumOrderLineResult.Close()
	//sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from STOCK
	sumStockResult, err := db.Query(`SELECT sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) FROM stock`)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
	}
	var sumSQUANTITYResult int
	var sumSYTDResult float64
	var sumSORDERCNTResult int
	var sumSREMOTECNTResult int

	if sumStockResult.Next() {
		err = sumStockResult.Scan(&sumSQUANTITYResult, &sumSYTDResult, &sumSORDERCNTResult, &sumSREMOTECNTResult)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			fmt.Println(err.Error())
			return
		}
		temp = []string{fmt.Sprintf("%v", sumSQUANTITYResult)}
		stats = append(stats, temp)
		temp = []string{fmt.Sprintf("%f", sumSYTDResult)}
		stats = append(stats, temp)
		temp = []string{fmt.Sprintf("%v", sumSORDERCNTResult)}
		stats = append(stats, temp)
		temp = []string{fmt.Sprintf("%v", sumSREMOTECNTResult)}
		stats = append(stats, temp)
	}

	// Output to csv

	csvFile, err := os.Create("./results/dbstate.csv")

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	csvwriter := csv.NewWriter(csvFile)

	for _, empRow := range stats {
		_ = csvwriter.Write(empRow)
	}
	csvwriter.Flush()
	csvFile.Close()
}
