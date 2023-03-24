package cql_transaction

import (
	mylog "Distributed_DB_Project/log"
	"fmt"
	"strconv"
	"time"

	"github.com/yugabyte/gocql"
)

func ExcPopularItem(session *gocql.Session, wid int, did int, l int) {
	// Begin the transaction
	mylog.Logger.INFO("Popular Item Transaction")
	var err error
	var dNextOId int

	statementN := "select D_NEXT_O_ID from District where D_W_ID=? and D_ID=?"
	statementS := "select O_ID, O_ENTRY_D, O_C_ID from orders where O_W_ID=? and O_D_ID=? and O_ID in (%s)"
	// statementS := "select O_ID, O_ENTRY_D, O_C_ID from orders where O_W_ID=? and O_D_ID=? and O_ID >= ? and O_ID <= ?"
	statementMaxQnty := "select OL_QUANTITY from order_quantity_item where W_ID = ? and D_ID = ? and O_ID = ? order by OL_QUANTITY DESC limit 1"
	statementP := "select OL_I_ID from order_quantity_item where W_ID = ? and D_ID = ? and O_ID = ? and OL_QUANTITY = ?"

	err = session.Query(statementN, wid, did).Scan(&dNextOId)
	if err != nil {
		mylog.Logger.ERRORf("Query Scan error [%s]", err.Error())
	}

	var lastOids string
	for i := 0; i < l; i++ {
		if i == l-1 {
			lastOids += strconv.Itoa(dNextOId)
			break
		}
		lastOids += (strconv.Itoa(dNextOId) + ",")
		dNextOId--
	}
	scanner := session.Query(fmt.Sprintf(statementS, lastOids), wid, did).Iter().Scanner()

	var oids, ocids []int
	var oEntryDs []time.Time
	for scanner.Next() {
		var oid, ocid int
		var oEntryD time.Time
		err = scanner.Scan(&oid, &oEntryD, &ocid)
		if err != nil {
			mylog.Logger.ERRORf("Query Scan error [%s]", err.Error())
		}
		oids = append(oids, oid)
		ocids = append(ocids, ocid)
		oEntryDs = append(oEntryDs, oEntryD)
	}

	var maxQs []int
	for _, oid := range oids {
		scanner := session.Query(statementMaxQnty, wid, did, oid).Iter().Scanner()
		for scanner.Next() {
			var maxQ int
			err = scanner.Scan(&maxQ)
			if err != nil {
				mylog.Logger.ERRORf("Query Scan error [%s]", err.Error())
			}
			maxQs = append(maxQs, maxQ)
		}
	}

	var oliidsPerOrder []map[int]struct{}
	for i, oid := range oids {
		scanner := session.Query(statementP, wid, did, oid, maxQs[i]).Iter().Scanner()
		oliids := make(map[int]struct{})
		for scanner.Next() {
			var oliid int
			err = scanner.Scan(&oliid)
			if err != nil {
				mylog.Logger.ERRORf("Query Scan error [%s]", err.Error())
			}
			oliids[oliid] = struct{}{}
		}
		oliidsPerOrder = append(oliidsPerOrder, oliids)
	}

	// Get item name
	iidToiname := make(map[int]string)
	for _, oliids := range oliidsPerOrder {
		for oliid := range oliids {
			if _, ok := iidToiname[oliid]; ok {
				continue
			}
			var iName string
			err = session.Query("select i_name from item where i_id = ?", oliid).Scan(&iName)
			if err != nil {
				mylog.Logger.ERRORf("Query Scan error [%s]", err.Error())
			}
			iidToiname[oliid] = iName
		}
	}

	// Get percentage
	percentages := make(map[string]float32)
	for iid, iname := range iidToiname {
		cnt := 0
		for _, oliids := range oliidsPerOrder {
			if _, ok := oliids[iid]; ok {
				cnt++
			}
		}
		percentages[iname] = float32(cnt) / float32(l)
	}

	// Get customer names
	var cNames []string
	for _, ocid := range ocids {
		var cF, cM, cL string
		err = session.Query("select C_FIRST, C_MIDDLE, C_LAST from customer where C_ID = ?", ocid).Scan(&cF, &cM, &cL)
		if err != nil {
			mylog.Logger.ERRORf("Query Scan error [%s]", err.Error())
		}
		cNames = append(cNames, fmt.Sprintf("(%s, %s, %s)", cF, cM, cL))
	}

	// Output info
	fmt.Println("[Popular Item Transaction]")
	fmt.Printf("(W_ID, D_ID) = (%d, %d)\nL = %d\n", wid, did, l)

	for i, oid := range oids {
		fmt.Printf("O_ID = %d, O_ENTRY_D = %s\n(C_FIRST, C_MIDDLE, C_LAST) = %s\n", oid, oEntryDs[i], cNames[i])
		oliids := oliidsPerOrder[i]
		for oliid := range oliids {
			fmt.Printf("I_NAME = %s, OL_QUANTITY = %d\n", iidToiname[oliid], maxQs[i])
		}
	}

	for iname, pcnt := range percentages {
		fmt.Printf("I_NAME = %s, PERCENTAGE = %f\n", iname, pcnt)
	}
}
