package cql_transaction

import (
	mylog "Distributed_DB_Project/log"
	"fmt"
	"github.com/yugabyte/gocql"
	"log"
	"strconv"
)

func ExecStockLevel(session *gocql.Session, warehouseID int, districtID int, threshold float32, last int) {
	var err error
	districtScanner := session.Query(`SELECT D_NEXT_O_ID FROM district
            								WHERE D_W_ID = ? and D_ID = ?`, warehouseID, districtID).Iter().Scanner()

	var Next_number int
	for districtScanner.Next() {

		err = districtScanner.Scan(&Next_number)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]", err.Error())
			log.Fatal(err.Error())
			return
		}
	}
	var statementS string = "select O_ID from orders where O_W_ID=? and O_D_ID=? and O_ID in (%s)"

	var lastOids string
	for i := Next_number - last; i < Next_number+1; i++ {
		if i == Next_number {
			lastOids += strconv.Itoa(i)
			break
		}
		lastOids += (strconv.Itoa(i) + ",")
	}
	orderScanner := session.Query(fmt.Sprintf(statementS, lastOids), warehouseID, districtID).Iter().Scanner()

	orders := make([]int, 0)
	var OrderNumber int
	for orderScanner.Next() {

		err = orderScanner.Scan(&OrderNumber)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]", err.Error())
			log.Fatal(err.Error())
			return
		}

		orders = append(orders, OrderNumber)

	}

	items := make([]int, 0)

	for i := 0; i < len(orders); i++ {
		orderlineScanner := session.Query(`SELECT OL_I_ID FROM orderLine
            								WHERE OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?`, warehouseID, districtID, orders[i]).Iter().Scanner()

		var ItemNumber int
		for orderlineScanner.Next() {
			err = orderlineScanner.Scan(&ItemNumber)

			if err != nil {
				mylog.Logger.ERRORf("Scan Error [%s]", err.Error())
				log.Fatal(err.Error())
				return
			}
			items = append(items, ItemNumber)
		}
	}
	var total int
	for j := 0; j < len(items); j++ {
		stockScanner := session.Query(`SELECT S_QUANTITY FROM stock
            								WHERE S_W_ID = ? and S_I_ID = ?`, warehouseID, items[j]).Iter().Scanner()

		var StockQuantity float32
		for stockScanner.Next() {
			err = stockScanner.Scan(&StockQuantity)
			if err != nil {
				mylog.Logger.ERRORf("Scan Error [%s]", err.Error())
				log.Fatal(err.Error())
				return
			}
			if StockQuantity < threshold {
				total = total + 1
			}
		}
	}

	fmt.Printf("total number of items below the threshold: (%d)\n", total)
	mylog.Logger.INFO("Stock-Level Transaction Completed")
}
