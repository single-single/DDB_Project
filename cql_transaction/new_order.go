package cql_transaction

import (
	mylog "Distributed_DB_Project/log"
	"context"
	"fmt"
	"github.com/yugabyte/gocql"
	"log"
	"os"
	"strconv"
	"time"
)

func ExecNewOrder(session *gocql.Session, warehouseID int, districtID int, customerID int, numItems int, itemNum []int, suppWarehouse []int, quantity []int) {

	mylog.Logger.INFO("NewOrder Transaction")

	if numItems > 20 {
		mylog.Logger.ERRORf("The Item number should not exceed 20")
		return
	}

	var err error
	ctx := context.Background()

	// Get District Information
	var orderNumber int
	var districtTax float32
	districtScanner := session.Query("SELECT D_NEXT_O_ID, D_TAX FROM district WHERE D_W_ID = ? and D_ID = ? ", warehouseID, districtID).Iter().Scanner()
	for districtScanner.Next() {
		err = districtScanner.Scan(&orderNumber, &districtTax)
		if err != nil {
			mylog.Logger.ERRORf("Query Scan Customer error [%s]", err.Error())
		}
	}

	var allLocal float32 = 1
	for _, id := range suppWarehouse {
		if id != warehouseID {
			allLocal = 0
			break
		}
	}
	entryDate := time.Now()

	b1 := session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	b1.Entries = append(b1.Entries, gocql.BatchEntry{
		Stmt:       "UPDATE district SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = ? and D_ID = ? ",
		Args:       []interface{}{warehouseID, districtID},
		Idempotent: true,
	})
	b1.Entries = append(b1.Entries, gocql.BatchEntry{
		Stmt: `INSERT INTO orders (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) 
						VALUES (?, ?, ?, ?, ?, ?, ?)`,
		Args:       []interface{}{orderNumber, districtID, warehouseID, customerID, entryDate, float32(numItems), allLocal},
		Idempotent: true,
	})
	err = session.ExecuteBatch(b1)
	if err != nil {
		mylog.Logger.ERRORf("ExecuteBatch Error [%s]", err.Error())
		log.Printf("ExecuteBatch Error [%s]\n", err.Error())
		return
	}

	var totalAmount float32 = 0

	var customerLast string
	var customerCredit string
	var customerDiscount float32
	var warehouseTax float32
	var itemPrice float32
	var itemName string

	var itemNames []string
	var orderLineAmount []float32
	var stockQuantities []float32

	for i := 0; i < numItems; i++ {
		// Get Stock Information
		var stockQuantity float32
		var stockDistInfo string
		districtIDStr := strconv.Itoa(districtID)
		if len(districtIDStr) == 1 {
			districtIDStr = "0" + districtIDStr
		}
		selectStockQuery := "SELECT S_QUANTITY, S_DIST_" + districtIDStr + " FROM stock WHERE S_W_ID = ? and S_I_ID = ? "
		stockScanner := session.Query(selectStockQuery, suppWarehouse[i], itemNum[i]).Iter().Scanner()
		for stockScanner.Next() {
			err = stockScanner.Scan(&stockQuantity, &stockDistInfo)
			if err != nil {
				mylog.Logger.ERRORf("Query Scan Stock error [%s]", err.Error())
			}
		}

		newQuantity := stockQuantity - float32(quantity[i])

		if newQuantity < 10 {
			newQuantity += 100
		}

		stockQuantities = append(stockQuantities, newQuantity)

		ifRemote := 0
		if suppWarehouse[i] != warehouseID {
			ifRemote = 1
		}

		// Get Item Information
		itemScanner := session.Query("SELECT I_NAME, I_PRICE FROM item WHERE I_ID = ? ", itemNum[i]).Iter().Scanner()
		for itemScanner.Next() {
			err = itemScanner.Scan(&itemName, &itemPrice)
			if err != nil {
				mylog.Logger.ERRORf("Query Scan Item error [%s]", err.Error())
			}
		}
		itemNames = append(itemNames, itemName)

		itemAmount := float32(quantity[i]) * itemPrice
		orderLineAmount = append(orderLineAmount, itemAmount)

		totalAmount += itemAmount

		b2 := session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
		b2.Entries = append(b2.Entries, gocql.BatchEntry{
			Stmt: `UPDATE stock SET 
                    							S_QUANTITY = ` + strconv.FormatFloat(float64(newQuantity), 'E', -0, 32) + `, 
                    							S_YTD = S_YTD + 1, 
                    							S_ORDER_CNT = S_ORDER_CNT + 1,
                    							S_REMOTE_CNT = S_REMOTE_CNT + ` + strconv.Itoa(ifRemote) + `
                				WHERE S_W_ID = ? and S_I_ID = ? `,
			Args:       []interface{}{suppWarehouse[i], itemNum[i]},
			Idempotent: true,
		})
		b2.Entries = append(b2.Entries, gocql.BatchEntry{
			Stmt: `INSERT INTO orderLine (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, 
                       OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) 
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			Args: []interface{}{orderNumber, districtID, warehouseID, i, itemNum[i], suppWarehouse[i],
				quantity[i], itemAmount, stockDistInfo},
			Idempotent: true,
		})
		b2.Entries = append(b2.Entries, gocql.BatchEntry{
			Stmt: `INSERT INTO item_order_customer (I_ID, W_ID, D_ID, O_ID, C_ID) 
						VALUES (?, ?, ?, ?, ?)`,
			Args:       []interface{}{itemNum[i], warehouseID, districtID, orderNumber, customerID},
			Idempotent: true,
		})
		b2.Entries = append(b2.Entries, gocql.BatchEntry{
			Stmt: `INSERT INTO order_quantity_item (W_ID, D_ID, O_ID, OL_QUANTITY, OL_I_ID) 
						VALUES (?, ?, ?, ?, ?)`,
			Args:       []interface{}{warehouseID, districtID, orderNumber, quantity[i], itemNum[i]},
			Idempotent: true,
		})
		err = session.ExecuteBatch(b2)
		if err != nil {
			mylog.Logger.ERRORf("ExecuteBatch Error [%s]", err.Error())
			log.Printf("ExecuteBatch Error [%s]\n", err.Error())
			return
		}

		// Get Warehouse Information
		warehouseScanner := session.Query("SELECT W_TAX FROM warehouse WHERE W_ID = ? ", warehouseID).Iter().Scanner()
		for warehouseScanner.Next() {
			err = warehouseScanner.Scan(&warehouseTax)
			if err != nil {
				mylog.Logger.ERRORf("Query Scan Warehouse error [%s]", err.Error())
			}
		}

		// Get Customer Information
		customerScanner := session.Query("SELECT C_LAST, C_CREDIT, C_DISCOUNT FROM customer WHERE C_W_ID = ? and C_D_ID = ? and C_ID = ? ",
			warehouseID, districtID, customerID).Iter().Scanner()
		for customerScanner.Next() {
			err = customerScanner.Scan(&customerLast, &customerCredit, &customerDiscount)
			if err != nil {
				mylog.Logger.ERRORf("Query Scan Customer error [%s]", err.Error())
			}
		}

		totalAmount = totalAmount * (1 + warehouseTax + districtTax) * (1 - customerDiscount)
	}

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
			"QUANTITY=%d\n, OL_AMOUNT=%f\n, S_QUANTITY=%f\n",
			itemNum[i], itemNames[i], suppWarehouse[i], quantity[i], orderLineAmount[i], stockQuantities[i])
	}
	return

}
