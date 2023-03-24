package transaction

import (
	mylog "Distributed_DB_Project/log"
	"database/sql"
	"fmt"
	"time"
)

type Datapoint struct {
	Timestamp time.Time
	Data      sql.NullString
}

func ExecOrderStatus(db *sql.DB, warehouseID int, districtID int, customerID int) {
	// Check customer info, including balance, YTD payment and payment count
	mylog.Logger.INFO("Order Status Transaction")
	defer func() {
		if r := recover(); r != nil {
			mylog.Logger.ERRORf("Recover: Fetal Error happened [%s]", r)
		}
	}()
	customerRows, err := db.Query(`SELECT  C_FIRST , C_MIDDLE, C_LAST,C_BALANCE FROM customer 
             								WHERE C_W_ID = $1 and C_D_ID = $2 and C_ID = $3`,
		warehouseID, districtID, customerID)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
	}
	defer customerRows.Close()
	var balance float32
	var first, middle, last string
	for customerRows.Next() {
		err = customerRows.Scan(&first, &middle, &last, &balance)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
		}
		fmt.Printf("To customer(%s-%s-%s), balance:%4.2f \n",
			first, middle, last, balance)
	}

	orderRows, err := db.Query(`SELECT  O_ID , O_ENTRY_D, O_CARRIER_ID FROM orders
             								WHERE O_W_ID = $1 and O_D_ID = $2 and O_C_ID = $3 ORDER BY O_ENTRY_D DESC`,
		warehouseID, districtID, customerID)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
	}
	defer orderRows.Close()
	var EntryDate time.Time
	var orderNum float64
	var CarrierIdentifier string
	for orderRows.Next() {
		err = orderRows.Scan(&orderNum, &EntryDate, &CarrierIdentifier)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
		}
		fmt.Printf("To customer last order(%f), the entry time is (%s) and carrier identifier are:(%s) \n",
			orderNum, EntryDate, CarrierIdentifier)
		break
	}

	orderlineRows, err := db.Query(`SELECT  OL_I_ID , OL_SUPPLY_W_ID, OL_QUANTITY,OL_AMOUNT,OL_DELIVERY_D FROM orderLine 
             								WHERE OL_W_ID = $1 and OL_D_ID = $2 and OL_O_ID = $3`,
		warehouseID, districtID, orderNum)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
	}
	defer orderlineRows.Close()
	var DeliveryDate *time.Time
	var ItemNumber, SupplyingWarehouseNumber float64
	var QuantityOrdered, TotalPrice float32
	for orderlineRows.Next() {
		err = orderlineRows.Scan(&ItemNumber, &SupplyingWarehouseNumber, &QuantityOrdered, &TotalPrice, &DeliveryDate)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())

		}
		fmt.Printf("item number: (%f), supplying warehouse number is: (%f), quantity ordered: %2.0f,total price for ordered item: %7.2f, data and time of delivery:(%s) \n",
			ItemNumber, SupplyingWarehouseNumber, QuantityOrdered, TotalPrice, DeliveryDate)
	}
	mylog.Logger.INFO("Order-Status Transaction Completed\n")
}
