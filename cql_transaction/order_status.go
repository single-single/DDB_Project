package cql_transaction

import (
	mylog "Distributed_DB_Project/log"
	"fmt"
	"github.com/yugabyte/gocql"
	"log"
	"time"
)

func ExecOrderStatus(session *gocql.Session, warehouseID int, districtID int, customerID int) {
	var err error
	customerScanner := session.Query(`SELECT  C_FIRST , C_MIDDLE, C_LAST,C_BALANCE FROM customer 
             								WHERE C_W_ID = ? and C_D_ID = ? and C_ID = ?`, warehouseID, districtID, customerID).Iter().Scanner()
	var balance float32
	var first, middle, last string
	for customerScanner.Next() {
		err = customerScanner.Scan(&first, &middle, &last, &balance)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]", err.Error())
			log.Fatal(err.Error())
			return
		}
		fmt.Printf("To customer(%s-%s-%s), balance:%4.2f \n",
			first, middle, last, balance)
	}

	orderScanner := session.Query(`SELECT  O_ID , O_ENTRY_D, O_CARRIER_ID FROM orders
             								WHERE O_W_ID = ? and O_D_ID = ? and O_C_ID = ?`, warehouseID, districtID, customerID).Iter().Scanner()

	var EntryDate time.Time
	var orderNum int
	var CarrierIdentifier string
	var latestDate time.Time
	var latest_CarrierIdentifier string
	var latest_orderNum int
	var index int = 0
	for orderScanner.Next() {
		err = orderScanner.Scan(&orderNum, &EntryDate, &CarrierIdentifier)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]", err.Error())
			log.Fatal(err.Error())
			return
		}
		if index == 0 || EntryDate.After(latestDate) {
			latestDate = EntryDate
			latest_orderNum = orderNum
			latest_CarrierIdentifier = CarrierIdentifier
		}
		index += 1
	}
	EntryDate = latestDate
	orderNum = latest_orderNum
	CarrierIdentifier = latest_CarrierIdentifier
	fmt.Printf("To customer last order(%d), the entry time is (%s) and carrier identifier are:(%s) \n",
		orderNum, EntryDate, CarrierIdentifier)
	orderlineScanner := session.Query(`SELECT  OL_I_ID , OL_SUPPLY_W_ID, OL_QUANTITY,OL_AMOUNT,OL_DELIVERY_D FROM orderLine 
             								WHERE OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?`, warehouseID, districtID, orderNum).Iter().Scanner()

	var DeliveryDate time.Time
	var ItemNumber, SupplyingWarehouseNumber, QuantityOrdered int
	var TotalPrice float32
	for orderlineScanner.Next() {
		err = orderlineScanner.Scan(&ItemNumber, &SupplyingWarehouseNumber, &QuantityOrdered, &TotalPrice, &DeliveryDate)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]", err.Error())
			log.Fatal(err.Error())
			return
		}
		fmt.Printf("item number: (%d), supplying warehouse number is: (%d), quantity ordered: %2.0f,total price for ordered item: %7.2f, data and time of delivery:(%s) \n",
			ItemNumber, SupplyingWarehouseNumber, QuantityOrdered, TotalPrice, DeliveryDate)
	}
	mylog.Logger.INFO("Order-Status Transaction Completed\n")
}
