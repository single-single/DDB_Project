package cql_transaction

import (
	"Distributed_DB_Project/entity"
	mylog "Distributed_DB_Project/log"
	"container/heap"
	"fmt"

	"github.com/yugabyte/gocql"
)

const (
	nTop  = 10 // top N balance
	numWH = 10 // number of warehouse
	numDT = 10 // number of district
)

// a min-heap of customer sorted by balance
type CustomerHeap []entity.Customer

func (h CustomerHeap) Len() int           { return len(h) }
func (h CustomerHeap) Less(i, j int) bool { return h[i].C_BALANCE < h[j].C_BALANCE }
func (h CustomerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *CustomerHeap) Push(x interface{}) {
	*h = append(*h, x.(entity.Customer))
}
func (h *CustomerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func ExcTopBalance(session *gocql.Session) {
	// Begin the transaction
	mylog.Logger.INFO("Top Balance Transaction")
	fmt.Println("[Top Balance Transaction]")
	var err error
	customers := &CustomerHeap{}
	heap.Init(customers)
	for i := 1; i <= numWH; i++ {
		for j := 1; j <= numDT; j++ {
			cusScanner := session.Query(
				`select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, C_W_NAME, C_D_NAME
				from Customer
				where C_W_ID = ? and C_D_ID = ?`, i, j,
			).Iter().Scanner()

			for cusScanner.Next() {
				var customer entity.Customer
				err = cusScanner.Scan(
					&customer.C_FIRST,
					&customer.C_MIDDLE,
					&customer.C_LAST,
					&customer.C_BALANCE,
					&customer.C_W_NAME,
					&customer.C_D_NAME,
				)
				if err != nil {
					mylog.Logger.ERRORf("Query Scan Customer error [%s]", err.Error())
				}

				if customers.Len() < nTop {
					heap.Push(customers, customer)
				} else if (*customers)[0].C_BALANCE < customer.C_BALANCE {
					heap.Pop(customers)
					heap.Push(customers, customer)
				}
			}
		}
	}

	for customers.Len() > 0 {
		customer := heap.Pop(customers).(entity.Customer)
		// Print out info
		fmt.Printf("(C_FIRST, C_MIDDLE, C_LAST) = (%s, %s, %s)\n", customer.C_FIRST, customer.C_MIDDLE, customer.C_LAST)
		fmt.Printf("C_BALANCE = %f\n", customer.C_BALANCE)
		fmt.Printf("W_NAME = %s\n", customer.C_W_NAME)
		fmt.Printf("D_NAME = %s\n", customer.C_D_NAME)
	}
}
