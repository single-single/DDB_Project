package transaction

import (
	mylog "Distributed_DB_Project/log"
	"database/sql"
	"fmt"
	"time"
)

func ExecDelivery(db *sql.DB, warehouseID int, carrierID int) {
	mylog.Logger.INFO("Delivery Transaction")
	defer func() {
		if r := recover(); r != nil {
			mylog.Logger.ERRORf("Recover: Fetal Error happened [%s]", r)
		}
	}()
	tx, err := db.Begin()
	if err != nil {
		if tx != nil {
			_ = tx.Rollback()
			mylog.Logger.INFO("Transaction Abort")
		}
		mylog.Logger.ERRORf("Transaction begin failed, [%s]", err.Error())
		return
	}

	for districtID := 1; districtID <= 10; districtID++ {
		mylog.Logger.DEBUG("Transaction Session: Obtain Order Number")
		selectOrderSQL := `SELECT O_ID, O_C_ID
							FROM orders WHERE O_W_ID = $1 and O_D_ID = $2 and O_CARRIER_ID is NULL `

		orderRows, err := db.Query(selectOrderSQL, warehouseID, districtID)
		if err != nil {
			mylog.Logger.ERROR(err.Error())
		}
		var currOrderID, minOrderID, currCustomerID, minCustomerID int
		isFirstRow := true
		for orderRows.Next() {
			if isFirstRow {
				err = orderRows.Scan(&minOrderID, &minCustomerID)
				isFirstRow = false
			} else {
				err = orderRows.Scan(&currOrderID, &currCustomerID)
			}
			if err != nil {
				mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
				fmt.Println(err.Error())
			}
			if currOrderID < minOrderID && currOrderID != 0 {
				minOrderID = currOrderID
				minCustomerID = currCustomerID
			}
		}

		mylog.Logger.DEBUG("Transaction Session: Update Order")
		updateOrderSQL := "UPDATE orders SET O_CARRIER_ID = $1 WHERE O_W_ID = $2 and O_D_ID = $3 and O_ID = $4 "
		result, err := tx.Exec(updateOrderSQL, carrierID, warehouseID, districtID, minOrderID)
		if err != nil {
			_ = tx.Rollback()
			mylog.Logger.ERRORf("Update statement execution failed [%s]", err.Error())
			mylog.Logger.INFO("Transaction Abort")
			return
		}
		_, err = result.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			mylog.Logger.ERRORf("Update statement RowsAffected failed [%s]", err.Error())
			mylog.Logger.INFO("Transaction Abort")
			return
		}

		mylog.Logger.DEBUG("Transaction Session: Update Order Line")
		updateOrderLineSQL := `UPDATE orderLine SET OL_DELIVERY_D = $1 
							      WHERE OL_W_ID = $2 and OL_D_ID = $3 and OL_O_ID = $4`
		result, err = tx.Exec(updateOrderLineSQL, time.Now(), warehouseID, districtID, minOrderID)
		if err != nil {
			_ = tx.Rollback()
			mylog.Logger.ERRORf("Update statement execution failed [%s]", err.Error())
			mylog.Logger.INFO("Transaction Abort")
			return
		}
		_, err = result.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			mylog.Logger.ERRORf("Update statement RowsAffected failed [%s]", err.Error())
			mylog.Logger.INFO("Transaction Abort")
			return
		}

		mylog.Logger.DEBUG("Transaction Session: Update Customer")

		selectOrderLineSQL := `SELECT OL_AMOUNT
								FROM orderLine WHERE OL_W_ID = $1 and OL_D_ID = $2 and OL_O_ID = $3 `

		orderLineRows, err := db.Query(selectOrderLineSQL, warehouseID, districtID, minOrderID)
		if err != nil {
			mylog.Logger.ERROR(err.Error())
		}
		var amount, totalAmount float32
		totalAmount = 0
		for orderLineRows.Next() {
			err = orderLineRows.Scan(&amount)
			if err != nil {
				mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
				fmt.Println(err.Error())
			}
			totalAmount += amount
		}

		updateCustomerSQL := `UPDATE customer SET 
                    							C_BALANCE = C_BALANCE + $1, 
                    							C_DELIVERY_CNT = C_DELIVERY_CNT + 1 
                				WHERE C_W_ID = $2 and C_D_ID = $3 and C_ID = $4 `
		result, err = tx.Exec(updateCustomerSQL, totalAmount, warehouseID, districtID, minCustomerID)
		if err != nil {
			_ = tx.Rollback()
			mylog.Logger.ERRORf("Update statement execution failed [%s]", err.Error())
			mylog.Logger.INFO("Transaction Abort")
			return
		}
		_, err = result.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			mylog.Logger.ERRORf("Update statement RowsAffected failed [%s]", err.Error())
			mylog.Logger.INFO("Transaction Abort")
			return
		}
	}

	_ = tx.Commit()
	mylog.Logger.INFO("Delivery Transaction Commit")

}
