package cql_transaction

import (
	mylog "Distributed_DB_Project/log"
	"context"
	"github.com/yugabyte/gocql"
	"log"
	"strconv"
	"time"
)

func ExecDelivery(session *gocql.Session, warehouseID int, carrierID int) {
	mylog.Logger.INFO("Delivery Transaction")
	var err error
	ctx := context.Background()
	b := session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	for districtID := 1; districtID <= 10; districtID++ {
		// Get Order Information
		var currOrderID, minOrderID, currCustomerID, minCustomerID int
		orderScanner := session.Query(`SELECT 
    							O_ID, O_C_ID
							FROM orders WHERE O_W_ID = ? and O_D_ID = ? and O_CARRIER_ID = NULL `, warehouseID, districtID).Iter().Scanner()
		isFirstRow := true
		for orderScanner.Next() {
			if isFirstRow {
				err = orderScanner.Scan(&minOrderID, &minCustomerID)
				isFirstRow = false
			} else {
				err = orderScanner.Scan(&currOrderID, &currCustomerID)
			}
			if err != nil {
				mylog.Logger.ERRORf("Query Scan Customer error [%s]", err.Error())
			}
			if currOrderID < minOrderID && currOrderID != 0 {
				minOrderID = currOrderID
				minCustomerID = currCustomerID
			}
		}

		// Get OrderLine Information
		var orderLineNumber int
		var amount, totalAmount float32
		totalAmount = 0
		orderLineScanner := session.Query(`SELECT OL_AMOUNT, OL_NUMBER
							FROM orderLine WHERE OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ? `,
			warehouseID, districtID, minOrderID).Iter().Scanner()
		for orderLineScanner.Next() {
			err = orderLineScanner.Scan(&amount, &orderLineNumber)
			if err != nil {
				mylog.Logger.ERRORf("Query Scan Customer error [%s]", err.Error())
			}
			totalAmount += amount
		}

		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt:       "UPDATE orders SET O_CARRIER_ID = '" + strconv.Itoa(carrierID) + "' WHERE O_W_ID = ? and O_D_ID = ? and O_ID = ? ",
			Args:       []interface{}{warehouseID, districtID, minOrderID},
			Idempotent: true,
		})
		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt: `UPDATE orderLine SET 
                     				OL_DELIVERY_D = '` + time.Now().Format("2006-01-02") + `'  
								WHERE OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ? and OL_NUMBER = ?`,
			Args:       []interface{}{warehouseID, districtID, minOrderID, orderLineNumber},
			Idempotent: true,
		})
		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt: `UPDATE customer SET 
                    							C_BALANCE = C_BALANCE - ` + strconv.FormatFloat(float64(totalAmount), 'f', 2, 32) + `,
                    							C_DELIVERY_CNT = C_DELIVERY_CNT + 1 
                				WHERE C_W_ID = ? and C_D_ID = ? and C_ID = ? `,
			Args:       []interface{}{warehouseID, districtID, minCustomerID},
			Idempotent: true,
		})
		err = session.ExecuteBatch(b)
		if err != nil {
			mylog.Logger.ERRORf("ExecuteBatch Error [%s]", err.Error())
			log.Printf("ExecuteBatch Error [%s]\n", err.Error())
			return
		}

	}

}
