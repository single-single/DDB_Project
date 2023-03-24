package cql_transaction

import (
	"Distributed_DB_Project/entity"
	mylog "Distributed_DB_Project/log"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/yugabyte/gocql"
)

func CheckPayment(session *gocql.Session, warehouseID int, districtID int, customerID int) {
	// Read only transaction, so no need to begin a transaction
	// Check YTD
	var err error
	warehouseScanner := session.Query("SELECT W_YTD FROM warehouse WHERE W_ID = ?", warehouseID).Iter().Scanner()
	var wYTD float32
	for warehouseScanner.Next() {
		err = warehouseScanner.Scan(&wYTD)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]", err.Error())
			log.Fatal(err.Error())
			return
		}
		log.Printf("W_YTD of warehouse(ID:%d) is %v\n", warehouseID, wYTD)
	}

	districtScanner := session.Query(`SELECT D_YTD FROM district
	        								WHERE D_W_ID = ? and D_ID = ? `, warehouseID, districtID).Iter().Scanner()
	var dYTD float32
	for districtScanner.Next() {
		err = districtScanner.Scan(&dYTD)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]", err.Error())
			log.Fatal(err.Error())
			return
		}
		log.Printf("D_YTD of district(ID:%d-%d) is %v\n", warehouseID, districtID, dYTD)
	}

	//// Check customer info, including balance, YTD payment and payment count
	customerScanner := session.Query(`SELECT  C_BALANCE , C_YTD_PAYMENT, C_PAYMENT_CNT FROM customer_by_balance
	         								WHERE C_W_ID = ? and C_D_ID = ? and C_ID = ?`,
		warehouseID, districtID, customerID).Iter().Scanner()
	var balance float32
	var ytdPayment float32
	var paymentCNT int64
	for customerScanner.Next() {
		err = customerScanner.Scan(&balance, &ytdPayment, &paymentCNT)
		if err != nil {
			mylog.Logger.ERRORf("Scan Error [%s]", err.Error())
			log.Fatal(err.Error())
			return
		}
		log.Printf("To customer(%d-%d-%d), balance:%v , ytdPayment:%4.2f, paymentCNT:%4d\n",
			warehouseID, districtID, customerID, balance, ytdPayment, paymentCNT)
	}
	mylog.Logger.INFO("Finish CheckInfo Payment\n")
}

func ExecPayment(session *gocql.Session, warehouseID int, districtID int, customerID int, amount float64) {
	// Begin the transaction
	mylog.Logger.INFO("Payment Transaction")
	var err error
	ctx := context.Background()
	b := session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	amountString := strconv.FormatFloat(amount, 'f', 5, 64)
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt:       "UPDATE warehouse SET W_YTD = W_YTD + " + amountString + " WHERE W_ID = ?",
		Args:       []interface{}{warehouseID},
		Idempotent: true,
	})
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt:       "UPDATE district SET D_YTD = D_YTD + " + amountString + " WHERE D_W_ID = ? and D_ID =? ",
		Args:       []interface{}{warehouseID, districtID},
		Idempotent: true,
	})
	b.Entries = append(b.Entries, gocql.BatchEntry{

		Stmt: `UPDATE customer SET 
                    							C_BALANCE = C_BALANCE - ` + amountString + `, 
                    							C_YTD_PAYMENT = C_YTD_PAYMENT + ` + amountString + `,
                    							C_PAYMENT_CNT = C_PAYMENT_CNT + 1
                				WHERE C_W_ID = ? and C_D_ID = ? and C_ID = ? `,
		Args:       []interface{}{warehouseID, districtID, customerID},
		Idempotent: true,
	})
	err = session.ExecuteBatch(b)
	if err != nil {
		mylog.Logger.ERRORf("ExecuteBatch Error [%s]", err.Error())
		log.Printf("ExecuteBatch Error [%s]\n", err.Error())
		return
	}

	// Get Customer Information
	var customer entity.Customer
	customerScanner := session.Query(`SELECT 
    							C_FIRST, C_MIDDLE, C_LAST,
    							C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
    							C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE
							FROM customer_by_balance WHERE C_W_ID = ? and C_D_ID = ? and C_ID = ? `,
		warehouseID, districtID, customerID).Iter().Scanner()
	for customerScanner.Next() {
		err = customerScanner.Scan(&customer.C_FIRST, &customer.C_MIDDLE, &customer.C_LAST,
			&customer.C_STREET_1, &customer.C_STREET_2, &customer.C_CITY, &customer.C_STATE, &customer.C_ZIP,
			&customer.C_PHONE, &customer.C_SINCE, &customer.C_CREDIT, &customer.C_CREDIT_LIM, &customer.C_DISCOUNT, &customer.C_BALANCE)
		if err != nil {
			mylog.Logger.ERRORf("Query Scan Customer error [%s]", err.Error())
		}
	}

	// Get Warehouse Information
	var warehouse entity.Warehouse
	warehouseScanner := session.Query(`SELECT 
    							W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
							FROM warehouse WHERE W_ID = ?`, warehouseID).Iter().Scanner()
	for warehouseScanner.Next() {
		err = warehouseScanner.Scan(&warehouse.W_STREET_1, &warehouse.W_STREET_2,
			&warehouse.W_CITY, &warehouse.W_STATE, &warehouse.W_ZIP)
		if err != nil {
			mylog.Logger.ERRORf("Query Scan Customer error [%s]", err.Error())
		}
	}

	// Get Warehouse Information
	var district entity.District
	districtScanner := session.Query(`SELECT 
    							D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
							FROM district WHERE D_W_ID = ? and D_ID = ? `, warehouseID, districtID).Iter().Scanner()
	for districtScanner.Next() {
		err = districtScanner.Scan(&district.D_STREET_1, &district.D_STREET_2,
			&district.D_CITY, &district.D_STATE, &district.D_ZIP)
		if err != nil {
			mylog.Logger.ERRORf("Query Scan Customer error [%s]", err.Error())
		}
	}

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
