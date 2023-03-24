package entity

import "time"

type Order struct {
	O_W_ID			int 
    O_D_ID 			int 
    O_ID 			int 
    O_C_ID 			int 
    O_CARRIER_ID	string
    O_OL_CNT 		float32
    O_ALL_LOCAL 	float32
    O_ENTRY_D 		time.Time
}
