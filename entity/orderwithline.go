package entity

import "time"

type OrderWithLine struct {
	O_W_ID      int
	O_D_ID      int
	O_C_ID      int
	O_ID        int
	O_ENTRY_D   time.Time
	OL_I_ID     int
	OL_QUANTITY int
}
