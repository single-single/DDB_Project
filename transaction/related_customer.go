package transaction

import (
	_ "Distributed_DB_Project/entity"
	mylog "Distributed_DB_Project/log"
	"database/sql"
	"fmt"
	"log"
	"strconv"
)

func RelatedCustomer(db *sql.DB, customerWarehouseID int, customerDistrictID int, customerID int) {
	// Output customer identifier
	mylog.Logger.INFO("Related Customer Transaction")
	defer func() {
		if r := recover(); r != nil {
			mylog.Logger.ERRORf("Recover: Fetal Error happened [%s]", r)
		}
	}()
	log.Printf("Querying related customer(s) for customer %d from Warehouse %d and District %d.\n ", customerID, customerWarehouseID, customerDistrictID)
	// Query for the CID of related customers
	// Step 1: get the order IDs that are related to the customer
	getOrders, err := db.Query(
		`SELECT O_ID 
			FROM orders
			WHERE O_W_ID = $1::INT
			AND O_D_ID = $2
			AND O_C_ID = $3`,
		customerWarehouseID,
		customerDistrictID,
		customerID)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
		return
	}
	defer getOrders.Close()
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
	// For each order, find out the items inside and find related customer based on this set of
	// ordered items

	for i := 0; i < len(orders); i++ {
		rItem := make([]int, 0)
		getRItemID, err := db.Query(
			`SELECT OL_I_ID 
							FROM orderLine
		    				WHERE OL_W_ID = $1 and OL_D_ID = $2 and OL_O_ID = $3`,
			customerWarehouseID,
			customerDistrictID,
			orders[i])
		if err != nil {
			mylog.Logger.ERROR(err.Error())
			return
		}
		var rItemID int
		for getRItemID.Next() {
			err = getRItemID.Scan(&rItemID)
			if err != nil {
				mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
				log.Println(err.Error())
				return
			}
			// Append the order IDs to the array
			rItem = append(rItem, rItemID)
		}
		getRItemID.Close()
		var rItemIDString string
		for j := 0; j < len(rItem); j++ {
			if j == len(rItem)-1 {
				rItemIDString += strconv.Itoa(rItem[j])
				break
			}
			rItemIDString += (strconv.Itoa(rItem[j]) + ",")
		}
		relatedOrder, err := db.Query(
			fmt.Sprintf(`SELECT
							OL_W_ID, OL_D_ID, OL_O_ID, COUNT(OL_I_ID) AS commonItemCount
						FROM orderLine 
						WHERE OL_I_ID IN (%s)
						GROUP BY OL_W_ID, OL_D_ID, OL_O_ID`,
				rItemIDString))
		if err != nil {
			mylog.Logger.ERROR(err.Error())
		}
		// from related order, identify related order
		var relatedWarehouseID int
		var relatedDistrictID int
		var relatedOrderID int
		var commonItemCount2 int
		for relatedOrder.Next() {
			err = relatedOrder.Scan(&relatedWarehouseID, &relatedDistrictID, &relatedOrderID, &commonItemCount2)
			if err != nil {
				mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
				fmt.Println(err.Error())
			}
			if commonItemCount2 >= 2 {
				// Find the customer ID of related customer
				relatedCustomerID, err := db.Query(
					`SELECT DISTINCT O_C_ID
					FROM orders
					WHERE O_W_ID = $1::int
					AND O_D_ID = $2
					AND O_ID = $3
					AND O_C_ID != $4
					AND O_W_ID != $5`,
					relatedWarehouseID,
					relatedDistrictID,
					relatedOrderID,
					customerID,
					customerWarehouseID)
				if err != nil {
					mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
					fmt.Println(err.Error())
				}
				var rCustomerID int
				for relatedCustomerID.Next() {
					//var notVaild bool
					err = relatedCustomerID.Scan(&rCustomerID)
					if err != nil {
						mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
						fmt.Println(err.Error())
					}
					fmt.Printf("%d is a related customer\n", rCustomerID)
				}
				relatedCustomerID.Close()
			}
		}
		relatedOrder.Close()
	}
	log.Println("End of query.")
	mylog.Logger.INFO("Related Customer Transaction Completed")
}
