package cql_transaction

import (
	_ "Distributed_DB_Project/entity"
	mylog "Distributed_DB_Project/log"
	"fmt"
	"log"

	"github.com/yugabyte/gocql"
)

func RelatedCustomer(session *gocql.Session, customerWarehouseID int, customerDistrictID int, customerID int) {
	// Output customer identifier

	log.Printf("Querying related customer(s) for customer %d from Warehouse %d and District %d.\n ", customerID, customerWarehouseID, customerDistrictID)
	// Query for the CID of related customers
	// Step 1: get the order IDs that are related to the customer
	var err error
	getOrders := session.Query("SELECT O_ID FROM orders WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ?", customerWarehouseID, customerDistrictID, customerID).Iter().Scanner()
	//defer getOrders.Close()
	// Store the order IDs in an array
	orders := make([]int, 0)
	var orderNumber int
	for getOrders.Next() {
		err = getOrders.Scan(&orderNumber)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			fmt.Println(err.Error())
			return
		}
		// Append the order IDs to the array
		orders = append(orders, orderNumber)
	}
	type relatedOrderDetail struct {
		OL_W_ID int
		OL_D_ID int
		OL_O_ID int
	}
	// For each order, find out the items inside and find related customer based on this set of
	// ordered items
	for i := 0; i < len(orders); i++ {
		mp := map[relatedOrderDetail]int{}
		//items := make([]int, 0)
		var itemID int
		orderedItem := session.Query(`SELECT OL_I_ID 
			FROM orderLine
			WHERE OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?`, customerWarehouseID, customerDistrictID, orders[i]).Iter().Scanner()
		for orderedItem.Next() {
			err = orderedItem.Scan(&itemID)
			if err != nil {
				mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
				fmt.Println(err.Error())
				return
			}
			// Append the order IDs to the array
			var relatedWarehouseID int
			var relatedDistrictID int
			var relatedOrderID int
			var relatedCustomerID int
			rItemInfo := session.Query(`SELECT W_ID, D_ID, O_ID, C_ID FROM item_order_customer WHERE I_ID = ?`, itemID).Iter().Scanner()
			for rItemInfo.Next() {
				err = rItemInfo.Scan(&relatedWarehouseID, &relatedDistrictID, &relatedOrderID, &relatedCustomerID)
				if err != nil {
					mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
					fmt.Println(err.Error())
					return
				}
				curOrder := relatedOrderDetail{relatedWarehouseID, relatedDistrictID, relatedOrderID}

				if mp[curOrder] != 0 && mp[curOrder] != itemID && relatedWarehouseID != customerWarehouseID && relatedCustomerID != customerID {
					fmt.Printf("%v is a related customer.", relatedCustomerID)
				} else if mp[curOrder] == 0 && relatedWarehouseID != customerWarehouseID && relatedCustomerID != customerID {
					mp[curOrder] = itemID
				}
			}
		}
	}
}
