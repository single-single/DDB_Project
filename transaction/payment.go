package transaction

import (
	"Distributed_DB_Project/entity"
	mylog "Distributed_DB_Project/log"
	"database/sql"
	"fmt"
	"log"
	"os"
)

func ExecPayment(db *sql.DB, warehouseID int, districtID int, customerID int, amount float64) {
	// Begin the transaction
	mylog.Logger.INFO("Payment Transaction")
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

	updateWarehouseSQL := "UPDATE warehouse SET W_YTD = W_YTD + $1 WHERE W_ID = $2"
	result, err := tx.Exec(updateWarehouseSQL, amount, warehouseID)
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
	mylog.Logger.DEBUG("Transaction Session: Update Warehouse")

	updateDistrictSQL := "UPDATE district SET D_YTD = D_YTD + $1 WHERE D_W_ID = $2 and D_ID = $3 "
	result, err = tx.Exec(updateDistrictSQL, amount, warehouseID, districtID)
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
	mylog.Logger.DEBUG("Transaction Session: Update District")

	updateCustomerSQL := `UPDATE customer SET 
                    							C_BALANCE = C_BALANCE - $1, 
                    							C_YTD_PAYMENT = C_YTD_PAYMENT + $2, 
                    							C_PAYMENT_CNT = C_PAYMENT_CNT + 1
                				WHERE C_W_ID = $3 and C_D_ID = $4 and C_ID = $5 `
	result, err = tx.Exec(updateCustomerSQL, amount, amount, warehouseID, districtID, customerID)
	if err != nil {
		mylog.Logger.ERRORf("Update statement execution failed [%s]", err.Error())
		err = tx.Rollback()
		if err != nil {
			mylog.Logger.ERRORf("RollBack Error [%s]", err.Error())
		}
		mylog.Logger.INFO("Transaction Abort")
		return
	}
	_, err = result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		mylog.Logger.ERRORf("Update statement RowsAffected failed [%s]", err.Error())
		return
	}
	mylog.Logger.DEBUG("Transaction Session: Update Customer")

	// Get Customer Information
	var customer entity.Customer
	selectCustomerSQL := `SELECT 
    							C_FIRST, C_MIDDLE, C_LAST,
    							C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
    							C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE
							FROM customer WHERE C_W_ID = $1 and C_D_ID = $2 and C_ID = $3 `
	row := tx.QueryRow(selectCustomerSQL, warehouseID, districtID, customerID)
	err = row.Scan(&customer.C_FIRST, &customer.C_MIDDLE, &customer.C_LAST,
		&customer.C_STREET_1, &customer.C_STREET_2, &customer.C_CITY, &customer.C_STATE, &customer.C_ZIP,
		&customer.C_PHONE, &customer.C_SINCE, &customer.C_CREDIT, &customer.C_CREDIT_LIM, &customer.C_DISCOUNT, &customer.C_BALANCE)
	if err != nil {
		tx.Rollback()
		mylog.Logger.ERRORf("Query Scan Customer error [%s]", err.Error())
		return
	}

	// Get Warehouse Information
	var warehouse entity.Warehouse
	selectWarehouseSQL := `SELECT 
    							W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
							FROM warehouse WHERE W_ID = $1`
	row = tx.QueryRow(selectWarehouseSQL, warehouseID)
	err = row.Scan(&warehouse.W_STREET_1, &warehouse.W_STREET_2, &warehouse.W_CITY, &warehouse.W_STATE, &warehouse.W_ZIP)
	if err != nil {
		tx.Rollback()
		mylog.Logger.ERRORf("Query Scan Warehouse error [%s]", err.Error())
		return
	}

	// Get Warehouse Information
	var district entity.District
	selectDistrictSQL := `SELECT 
    							D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
							FROM district WHERE D_W_ID = $1 and D_ID = $2`
	row = tx.QueryRow(selectDistrictSQL, warehouseID, districtID)
	err = row.Scan(&district.D_STREET_1, &district.D_STREET_2, &district.D_CITY, &district.D_STATE, &district.D_ZIP)
	if err != nil {
		tx.Rollback()
		mylog.Logger.ERRORf("Query Scan District error [%s]", err.Error())
		return
	}
	_ = tx.Commit()
	mylog.Logger.INFO("Payment Transaction Commit")
	// Output
	outputTitle := "[Payment Transaction Completed]\n"
	outputCustomer := fmt.Sprintf("For customer(%d-%d-%d):\nC_FIRST=%s,\n C_MIDDLE=%s,\n C_LAST=%s,\n C_STREET_1=%s,\n "+
		"C_STREET_2=%s,\n C_CITY=%s,\n C_STATE=%s,\n C_ZIP=%s,\n C_PHONE=%s,\n C_SINCE=%v,\n C_CREDIT=%s,\n"+
		"C_CREDIT_LIM=%f,\n C_DISCOUNT=%f,\n C_BALANCE=%f\n",
		warehouseID, districtID, customerID,
		customer.C_FIRST, customer.C_MIDDLE, customer.C_LAST,
		customer.C_STREET_1, customer.C_STREET_2, customer.C_CITY, customer.C_STATE, customer.C_ZIP,
		customer.C_PHONE, customer.C_SINCE, customer.C_CREDIT, customer.C_CREDIT_LIM, customer.C_DISCOUNT, customer.C_BALANCE)
	outputWarehouse := fmt.Sprintf("For warehouse(%d): W_STREET_1=%s,\n W_STREET_2=%s,\n W_CITY=%s,\n W_STATE=%s,\n W_ZIP=%s\n",
		warehouseID, warehouse.W_STREET_1, warehouse.W_STREET_2, warehouse.W_CITY, warehouse.W_STATE, warehouse.W_ZIP)
	outputDistrict := fmt.Sprintf("For district(%d): D_STREET_1=%s,\n D_STREET_2=%s,\n D_CITY=%s,\n D_STATE=%s,\n D_ZIP=%s\n",
		districtID, district.D_STREET_1, district.D_STREET_2, district.D_CITY, district.D_STATE, district.D_ZIP)
	os.Stdout.WriteString(outputTitle + outputCustomer + outputWarehouse + outputDistrict)
	return
}

func CheckPayment(db *sql.DB, warehouseID int, districtID int, customerID int) {
	// Read only transaction, so no need to begin a transaction
	// Check YTD
	//stmt := "SELECT W_YTD FROM warehouse WHERE W_ID = $1"
	warehouseRows, err := db.Query("SELECT W_YTD FROM warehouse WHERE W_ID = $1", warehouseID)
	//warehouseRows, err := db.Query("SELECT W_YTD FROM warehouse WHERE W_ID= 1")

	if err != nil {
		mylog.Logger.ERROR(err.Error())
		log.Fatal(err.Error())
		return
	}
	defer warehouseRows.Close()
	var wYtd float32
	for warehouseRows.Next() {
		err = warehouseRows.Scan(&wYtd)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			log.Println(err.Error())
		}
		log.Printf("W_YTD of warehouse(ID:%d) is %f\n", warehouseID, wYtd)
	}

	districtRows, err := db.Query(`SELECT D_YTD FROM district 
             								WHERE D_W_ID = $1 and D_ID = $2 `, warehouseID, districtID)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
	}
	defer districtRows.Close()
	var dYtd float32
	for districtRows.Next() {
		err = districtRows.Scan(&dYtd)
		log.Printf("D_YTD of district(ID:%d-%d) is %f\n", warehouseID, districtID, dYtd)
	}

	// Check customer info, including balance, YTD payment and payment count
	customerRows, err := db.Query(`SELECT  C_BALANCE , C_YTD_PAYMENT, C_PAYMENT_CNT FROM customer 
             								WHERE C_W_ID = $1 and C_D_ID = $2 and C_ID = $3`,
		warehouseID, districtID, customerID)
	if err != nil {
		mylog.Logger.ERROR(err.Error())
	}
	defer customerRows.Close()
	var balance, ytdPayment float32
	var paymentCNT int64
	for customerRows.Next() {
		err = customerRows.Scan(&balance, &ytdPayment, &paymentCNT)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			log.Println(err.Error())
		}
		log.Printf("To customer(%d-%d-%d), balance:%4.2f , ytdPayment:%4.2f, paymentCNT:%4d\n",
			warehouseID, districtID, customerID, balance, ytdPayment, paymentCNT)
	}
	mylog.Logger.INFO("Finish CheckInfo Payment\n")
}
