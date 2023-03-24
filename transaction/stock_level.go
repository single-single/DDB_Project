package transaction

import (
	mylog "Distributed_DB_Project/log"
	"database/sql"
	"fmt"
)

func ExecStockLevel(db *sql.DB, warehouseID int, districtID int, threshold int, last int) {
	mylog.Logger.INFO("Stock Level Transaction")
	defer func() {
		if r := recover(); r != nil {
			mylog.Logger.ERRORf("Recover: Fetal Error happened [%s]", r)
		}
	}()
	districtRow, err := db.Query(`SELECT D_NEXT_O_ID FROM district
            								WHERE D_W_ID = $1 and D_ID = $2`,
		warehouseID, districtID)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
		return
	}
	defer districtRow.Close()

	var Next_number int
	for districtRow.Next() {

		err = districtRow.Scan(&Next_number)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			return
		}
		break
	}

	//start of new implementation

	tx, err := db.Begin()
	rows, err := tx.Query(`select COUNT(DISTINCT OL_I_ID)
from 
	(select O_ID, O_W_ID, O_D_ID
	from orders
	where O_W_ID=$1 and O_D_ID=$2
		and (O_ID between $3 and $4)
	) o
left outer join orderLine
on O_ID = OL_O_ID and O_W_ID = OL_W_ID and O_D_ID = OL_D_ID
left outer join stock
on OL_I_ID = S_I_ID and OL_W_ID = S_W_ID
where S_QUANTITY < $5;`, warehouseID, districtID, Next_number-last, Next_number, threshold)
	if err != nil {
		_ = tx.Rollback()
		mylog.Logger.ERROR(err.Error())
		mylog.Logger.INFO("Transaction Abort")
		return
	}
	defer rows.Close()

	// end of new implementation

	//original version, not that slow when testing in main

	//orderRows, err := db.Query(`SELECT O_ID FROM orders
	//        								WHERE O_D_ID = $1 and O_W_ID = $2 and  (O_ID between $4 and $3)`,
	//	warehouseID, districtID, Next_number, Next_number-last)
	//if err != nil {
	//	mylog.Logger.ERROR(err.Error())
	//	return
	//}
	//defer orderRows.Close()
	//
	//orders := make([]float64, 0)
	//var OrderNumber float64
	//for orderRows.Next() {
	//
	//	err = orderRows.Scan(&OrderNumber)
	//	if err != nil {
	//		mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
	//		fmt.Println(err.Error())
	//		return
	//	}
	//
	//	orders = append(orders, OrderNumber)
	//
	//}
	//
	//items := make([]float64, 0)
	//
	//for i := 0; i < len(orders); i++ {
	//	orderlineRows, err := db.Query(`SELECT OL_I_ID FROM orderLine
	//        								WHERE OL_W_ID = $1 and OL_D_ID = $2 and OL_O_ID = $3`,
	//		warehouseID, districtID, orders[i])
	//	if err != nil {
	//		mylog.Logger.ERROR(err.Error())
	//		return
	//	}
	//
	//	var ItemNumber float64
	//	for orderlineRows.Next() {
	//		err = orderlineRows.Scan(&ItemNumber)
	//		if err != nil {
	//			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
	//			fmt.Println(err.Error())
	//			return
	//		}
	//		items = append(items, ItemNumber)
	//	}
	//	orderlineRows.Close()
	//}
	//
	//var total int
	//for j := 0; j < len(items); j++ {
	//	stockRows, err := db.Query(`SELECT S_QUANTITY FROM stock
	//        								WHERE S_W_ID = $1 and S_I_ID = $2`,
	//		warehouseID, items[j])
	//	if err != nil {
	//		mylog.Logger.ERROR(err.Error())
	//		return
	//	}
	//	var StockQuantity float64
	//	for stockRows.Next() {
	//		err = stockRows.Scan(&StockQuantity)
	//		if err != nil {
	//			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
	//			fmt.Println(err.Error())
	//			return
	//		}
	//		if StockQuantity < float64(threshold) {
	//			total = total + 1
	//		}
	//	}
	//	stockRows.Close()
	//}
	var total int
	for rows.Next() {
		err = rows.Scan(&total)
		if err != nil {
			tx.Rollback()
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			return
		}
	}
	fmt.Printf("total number of items below the threshold: (%d)\n", total)
	mylog.Logger.INFO("Stock-Level Transaction Completed")
}
