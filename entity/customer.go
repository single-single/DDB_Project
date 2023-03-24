package entity

import "time"

type Customer struct {
	C_W_ID         int
	C_D_ID         int
	C_ID           int
	C_FIRST        string
	C_MIDDLE       string
	C_LAST         string
	C_STREET_1     string
	C_STREET_2     string
	C_CITY         string
	C_STATE        string
	C_ZIP          string
	C_PHONE        string
	C_SINCE        time.Time
	C_CREDIT       string
	C_CREDIT_LIM   float32
	C_DISCOUNT     float32
	C_BALANCE      float32
	C_YTD_PAYMENT  float32
	C_PAYMENT_CNT  int
	C_DELIVERY_CNT int
	C_DATA         string
	C_W_NAME       string
	C_D_NAME       string
}
