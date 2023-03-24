package transaction

import (
	"Distributed_DB_Project/entity"
	mylog "Distributed_DB_Project/log"
	"database/sql"
	"fmt"
)

func ExcPopularItem(db *sql.DB, wid int, did int, l int) {
	// Begin the transaction
	mylog.Logger.INFO("Popular Item Transaction")
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
	statementTemp := `With ol as (
		Select O_ID, O_W_ID, O_D_ID, OL_QUANTITY, o_c_id, o_entry_d, ol_i_id
		From orderwithline
		Where O_W_ID = $1 and O_D_ID = $2 and (O_ID between $3 and $4))`

	// statementMaxQ := `Select ol1.o_id, ol1.o_c_id, ol1.o_entry_d, ol1.ol_i_id, ol1.ol_quantity
	// From ol ol1
	// Left join ol ol2
	// On ol1.o_w_id = ol2.o_w_id and ol1.o_d_id = ol2.o_d_id and ol1.o_id = ol2.o_id and ol1.ol_quantity < ol2.ol_quantity
	// Where ol2.ol_quantity is null;`
	statementAll := `Select o_id, o_c_id, o_entry_d, ol_i_id, ol_quantity, i_name, c_first, c_middle, c_last
	from
	(Select ol1.o_id, ol1.o_c_id, ol1.o_entry_d, ol1.ol_i_id, ol1.ol_quantity, ol1.o_w_id, ol1.o_d_id
		From ol ol1
		Left join ol ol2
		On ol1.o_w_id = ol2.o_w_id and ol1.o_d_id = ol2.o_d_id and ol1.o_id = ol2.o_id and ol1.ol_quantity < ol2.ol_quantity
		Where ol2.ol_quantity is null) ol
	Inner join (select i_id, i_name from item) i on ol_i_id = i_id
	Inner join (select c_w_id, c_d_id, c_id, c_first, c_middle, c_last from customer) c on c_w_id=o_w_id and c_d_id=o_d_id and c_id=o_c_id;`

	// Get value of D_NEXT_O_ID
	row := tx.QueryRow(`select D_NEXT_O_ID
						from District
						where D_W_ID=$1 and D_ID=$2`, wid, did)
	var dNextOId int
	err = row.Scan(&dNextOId)
	if err != nil {
		tx.Rollback()
		mylog.Logger.ERRORf("Fail to get D_NEXT_O_ID in T6: [%s]", err.Error())
		return
	}
	// Get customer, order and item info with max quantity
	// ordrows, err := tx.Query(statementTemp+statementMaxQ, wid, did, dNextOId-l, dNextOId)
	ordrows, err := tx.Query(statementTemp+statementAll, wid, did, dNextOId-l, dNextOId)
	if err != nil {
		_ = tx.Rollback()
		mylog.Logger.ERROR(err.Error())
		mylog.Logger.INFO("Transaction Abort")
		return
	}
	defer ordrows.Close()
	// Append required output info to a string
	output := "[Popular Item Transaction Completed]\n"
	output += fmt.Sprintf("W_ID=%d,\nD_ID=%d,\nL=%d,\n", wid, did, l)
	// var orderWithLines []entity.OrderWithLine
	for ordrows.Next() {
		var owl entity.OrderWithLine
		var iName string
		var cF, cM, cL string
		err := ordrows.Scan(
			&owl.O_ID,
			&owl.O_C_ID,
			&owl.O_ENTRY_D,
			&owl.OL_I_ID,
			&owl.OL_QUANTITY,
			&iName,
			&cF, &cM, &cL,
		)
		if err != nil {
			tx.Rollback()
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			return
		}
		output += fmt.Sprintf(
			"O_ID=%d, O_ENTRY_D=%s, C_FIRST=%s, C_MID=%s"+
				"C_LAST=%s, I_NAME=%s, MAX_QUANTITY=%d\n",
			owl.O_ID, owl.O_ENTRY_D.String(), cF, cM, cL, iName, owl.OL_QUANTITY)
	}
	pctrows, err := tx.Query(`
	select I_NAME, PERCENTAGE
	from
	(select OL_I_ID, COUNT(distinct OL_O_ID)::float/$5 as PERCENTAGE
	from
	(select O_W_ID, O_D_ID, O_ID, O_C_ID, O_ENTRY_D
		from orders
		where O_W_ID=$1 and O_D_ID=$2
			and (O_ID between $4 and $3)
		) s
	left outer join OrderLine
	on O_ID = OL_O_ID and O_W_ID = OL_W_ID and O_D_ID = OL_D_ID
	group by OL_I_ID) temp
	left outer join Item
	on I_ID = OL_I_ID;
	`, wid, did, dNextOId, dNextOId-l, l)

	if err != nil {
		_ = tx.Rollback()
		mylog.Logger.ERROR(err.Error())
		mylog.Logger.INFO("Transaction Abort")
		return
	}
	defer pctrows.Close()

	// Append required output info
	var percentage float32
	var iName string
	for pctrows.Next() {
		err = pctrows.Scan(
			&iName,
			&percentage,
		)
		if err != nil {
			tx.Rollback()
			mylog.Logger.ERRORf("Scan Error [%s]\n", err.Error())
			return
		}

		output += fmt.Sprintf(
			"I_NAME=%s, PERCENTAGE=%f\n",
			iName, percentage,
		)
	}

	fmt.Print(output)
	mylog.Logger.INFO("PopularItem Transaction Completed")
}
