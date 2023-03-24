package transaction

import (
	mylog "Distributed_DB_Project/log"
	"database/sql"
	"fmt"
)

func ExcTopBalance(db *sql.DB) {
	// Begin the transaction
	mylog.Logger.INFO("Top Balance Transaction")
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

	statement := `
	select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, W_NAME, D_NAME
	from
	(select C_FIRST,C_MIDDLE,C_LAST,C_BALANCE, c_w_id, c_d_id
	from Customer
	order by C_BALANCE desc
	limit 10) temp
	inner join District
	on c_w_id = D_W_ID and C_D_ID = D_ID
	inner join Warehouse
	on c_w_id = W_ID
	`
	rows, err := tx.Query(statement)
	if err != nil {
		_ = tx.Rollback()
		mylog.Logger.ERROR(err.Error())
		mylog.Logger.INFO("Transaction Abort")
		return
	}
	defer rows.Close()

	// Output required info
	output := "[Top Balance Transaction Completed]\n"
	var c_first, c_mid, c_last, w_name, d_name string
	var c_balance float32

	for rows.Next() {
		err = rows.Scan(
			&c_first, &c_mid, &c_last, &c_balance, &w_name, &d_name,
		)
		if err != nil {
			tx.Rollback()
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			return
		}

		output += fmt.Sprintf(
			"C_FIRST=%s, C_MID=%s, C_LAST=%s, C_BALANCE=%f, W_NAME=%s, D_NAME=%s\n",
			c_first, c_mid, c_last, c_balance, w_name, d_name,
		)
	}

	fmt.Print(output)
	mylog.Logger.INFO("Top Balance Transaction Completed")
}
