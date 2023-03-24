package performance_measurement

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
)

func Throughput_result() {
	// Read client.csv
	file, err := os.Open("./results/clients.csv")
	if err != nil {
		fmt.Println(err)
	}
	reader := csv.NewReader(file)
	records, _ := reader.ReadAll()
	// Print all the records in clients.csv
	fmt.Println(records)
	calculate_throughput(records)
}

func calculate_throughput(arr [][]string) {
	//res := [][]string{}
	//cur := []string{}
	var minThroughput float64
	var maxThroughput float64
	var avgThroughput float64
	minThroughput, _ = strconv.ParseFloat(arr[0][3], 64)
	for i := 0; i < len(arr); i++ {
		//fValue = arr[i][2]
		if s, err := strconv.ParseFloat(arr[i][3], 64); err == nil {
			if s >= maxThroughput {
				maxThroughput = s
			} else if s <= minThroughput {
				minThroughput = s
			}
			avgThroughput = avgThroughput + s
		}
	}
	avgThroughput = avgThroughput / 20

	stats := [][]string{{fmt.Sprintf("%f", minThroughput), fmt.Sprintf("%f", maxThroughput), fmt.Sprintf("%f", avgThroughput)}}
	// Write to csv
	csvFile, err := os.Create("./results/throughput.csv")

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	csvwriter := csv.NewWriter(csvFile)

	for _, empRow := range stats {
		_ = csvwriter.Write(empRow)
	}
	csvwriter.Flush()
	csvFile.Close()
	//cur = []string{fmt.Sprintf(arr[i][2])}
	//res = append(res, cur)
}
