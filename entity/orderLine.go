package entity

import "time"

type OrderLine struct {
	OL_W_ID 		int
	OL_D_ID 		int
	OL_O_ID 		int
	OL_NUMBER 		int
	OL_I_ID 		int
	OL_DELIVERY_D 	time.Time
	OL_AMOUNT 		float32
	OL_SUPPLY_W_ID 	int
	OL_QUANTITY 	float32
	OL_DIST_INFO 	string
}