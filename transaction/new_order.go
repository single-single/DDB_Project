package transaction

import (
	mylog "Distributed_DB_Project/log"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"
)

func ExecNewOrder(db *sql.DB, warehouseID int, districtID int, customerID int, numItems int, itemNum []int, suppWarehouse []int, quantity []int) {

	mylog.Logger.INFO("NewOrder Transaction")

	if numItems > 20 {
		mylog.Logger.ERRORf("The Item number should not exceed 20")
		return
	}

	mylog.Logger.INFO("NewOrder Transaction")
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

	mylog.Logger.DEBUG("Transaction Session: Obtain Order Number")
	var orderNumber int
	var districtTax float32
	selectDistrictSQL := "SELECT D_NEXT_O_ID, D_TAX FROM district WHERE D_W_ID = $1 and D_ID = $2 "
	row := tx.QueryRow(selectDistrictSQL, warehouseID, districtID)
	err = row.Scan(&orderNumber, &districtTax)
	if err != nil {
		tx.Rollback()
		mylog.Logger.ERRORf("Query Scan District error [%s]\n\t\twarehouseID = %d, districtID = %d",
			err.Error(), warehouseID, districtID)
		return
	}

	mylog.Logger.DEBUG("Transaction Session: Update District")
	updateDistrictSQL := "UPDATE district SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = $1 and D_ID = $2 "
	result, err := tx.Exec(updateDistrictSQL, warehouseID, districtID)
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

	mylog.Logger.DEBUG("Transaction Session: Create Order")
	var allLocal float32 = 1
	for _, id := range suppWarehouse {
		if id != warehouseID {
			allLocal = 0
			break
		}
	}
	insertOrderSQL := `INSERT INTO orders (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) 
						VALUES ($1, $2, $3, $4, $5, $6, $7)`
	entryDate := time.Now()
	result, err = tx.Exec(insertOrderSQL, orderNumber, districtID, warehouseID, customerID, entryDate, numItems, allLocal)
	if err != nil {
		_ = tx.Rollback()
		mylog.Logger.ERRORf("Insert statement execution failed [%s]", err.Error())
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

	mylog.Logger.DEBUG("Transaction Session: Place Order")

	var totalAmount float32 = 0

	var customerLast string
	var customerCredit string
	var customerDiscount float32
	var warehouseTax float32
	var itemPrice float32
	var itemName string

	var itemNames []string
	var orderLineAmount []float32
	var stockQuantities []int

	for i := 0; i < numItems; i++ {
		var stockQuantity int
		var stockDistInfo string
		districtIDStr := strconv.Itoa(districtID)
		if len(districtIDStr) == 1 {
			districtIDStr = "0" + districtIDStr
		}
		selectStockSQL := "SELECT S_QUANTITY, S_DIST_" + districtIDStr + " FROM stock WHERE S_W_ID = $1 and S_I_ID = $2 "
		fmt.Println(selectStockSQL)
		row := tx.QueryRow(selectStockSQL, suppWarehouse[i], itemNum[i])
		err = row.Scan(&stockQuantity, &stockDistInfo)
		if err != nil {
			tx.Rollback()
			mylog.Logger.ERRORf("Query Scan Stock error [%s]", err.Error())
			return
		}

		newQuantity := stockQuantity - quantity[i]

		if newQuantity < 10 {
			newQuantity += 100
		}

		stockQuantities = append(stockQuantities, newQuantity)

		ifRemote := 0
		if suppWarehouse[i] != warehouseID {
			ifRemote = 1
		}

		updateStockSQL := `UPDATE stock SET 
                    							S_QUANTITY = $1, 
                    							S_YTD = S_YTD + 1, 
                    							S_ORDER_CNT = S_ORDER_CNT + 1,
                    							S_REMOTE_CNT = S_REMOTE_CNT + $2
                				WHERE S_W_ID = $3 and S_I_ID = $4 `
		result, err = tx.Exec(updateStockSQL, newQuantity, ifRemote, suppWarehouse[i], itemNum[i])
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

		selectItemSQL := "SELECT I_NAME, I_PRICE FROM item WHERE I_ID = $1 "
		row = tx.QueryRow(selectItemSQL, itemNum[i])
		err = row.Scan(&itemName, &itemPrice)
		if err != nil {
			tx.Rollback()
			mylog.Logger.ERRORf("Query Scan Item error [%s]", err.Error())
			return
		}
		itemNames = append(itemNames, itemName)

		itemAmount := float32(quantity[i]) * itemPrice
		orderLineAmount = append(orderLineAmount, itemAmount)

		totalAmount += itemAmount

		insertOrderLineSQL := `INSERT INTO orderLine (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, 
                       OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) 
						VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
		result, err = tx.Exec(insertOrderLineSQL, orderNumber, districtID, warehouseID, i, itemNum[i], suppWarehouse[i],
			quantity[i], itemAmount, stockDistInfo)
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

		insertOrderWithLineSQL := `INSERT INTO orderwithline (O_W_ID, O_D_ID, O_ID, O_C_ID, O_OL_CNT, 
                       O_ALL_LOCAL, O_ENTRY_D, OL_NUMBER, OL_I_ID, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) 
						VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
		result, err = tx.Exec(insertOrderWithLineSQL, warehouseID, districtID, orderNumber, customerID, numItems, allLocal, entryDate,
			i, itemNum[i], itemAmount, suppWarehouse[i], quantity[i], stockDistInfo)
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

		selectWarehouseSQL := "SELECT W_TAX FROM warehouse WHERE W_ID = $1 "
		row = tx.QueryRow(selectWarehouseSQL, warehouseID)
		err = row.Scan(&warehouseTax)
		if err != nil {
			tx.Rollback()
			mylog.Logger.ERRORf("Query Scan Warehouse error [%s]", err.Error())
			return
		}

		selectCustomerSQL := "SELECT C_LAST, C_CREDIT, C_DISCOUNT FROM customer WHERE C_W_ID = $1 and C_D_ID = $2 and C_ID = $3 "
		row = tx.QueryRow(selectCustomerSQL, warehouseID, districtID, customerID)
		err = row.Scan(&customerLast, &customerCredit, &customerDiscount)
		if err != nil {
			tx.Rollback()
			mylog.Logger.ERRORf("Query Scan Customer error [%s]", err.Error())
			return
		}

		totalAmount = totalAmount * (1 + warehouseTax + districtTax) * (1 - customerDiscount)
	}

	_ = tx.Commit()
	mylog.Logger.INFO("New Order Transaction Commit")

	// Output
	outputTitle := "[New Order Transaction Completed]\n"
	outputCustomer := fmt.Sprintf("For customer(%d-%d-%d):\nC_LAST=%s,\n C_CREDIT=%s,\n C_DISCOUNT=%f\n",
		warehouseID, districtID, customerID, customerLast, customerCredit, customerDiscount)
	outputWarehouse := fmt.Sprintf("For warehouse(%d): W_TAX=%f\n", warehouseID, warehouseTax)
	outputDistrict := fmt.Sprintf("For district(%d): D_TAX=%f\n", districtID, districtTax)
	outputOrder := fmt.Sprintf("For Order(%d): O_ENTRY_D=%s\n", orderNumber, entryDate)
	outputItem := fmt.Sprintf("Number of items:%d, Total amount for order: %f\n", itemNum, totalAmount)
	os.Stdout.WriteString(outputTitle + outputCustomer + outputWarehouse + outputDistrict + outputOrder + outputItem)
	for i := 0; i < numItems; i++ {
		outputItem = fmt.Sprintf("For Item(%d): I_NAME=%s\n, SUPPLIER_WAREHOUSE=%d\n, "+
			"QUANTITY=%d\n, OL_AMOUNT=%f\n, S_QUANTITY=%d\n",
			itemNum[i], itemNames[i], suppWarehouse[i], quantity[i], orderLineAmount[i], stockQuantities[i])
	}
	return

}
