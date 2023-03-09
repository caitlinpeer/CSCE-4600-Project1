package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}

	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}



func SJFPrioritySchedule(w io.Writer, title string, processes []Process) { 
	var(
			n				=len(processes)
			i				int64
			j				int64
			pos				int64
			temp 			int64
			tempB			=make([]int64, n)
			tempPID			=make([]int64, n)
			tempPriority	=make([]int64, n)
			tempArr			=make([]int64, n)
			waiting_time   	=make([]int64, n)
			total			int64
			tat				=make([]int64, n)
			schedule        = make([][]string, len(processes))
			gantt           = make([]TimeSlice, 0)

			totalTurnaround	float64
			totalWaitTime	float64
			finalCompletion	int64
	)
	
	for i < int64(n){
		tempPID[i] = processes[i].ProcessID
		tempPriority[i] = processes[i].Priority
		tempB[i] = processes[i].BurstDuration
		tempArr[i] = processes[i].ArrivalTime

		tempPID[i] = i+1
		i++
	}

	i = 0
	for i < int64(n){
		pos = i
		j = i+1

		for j < int64(n){
			if tempPriority[j] < tempPriority[pos]{
			
				pos = j
			}
			j++
		}

		
		temp = tempPriority[i]
		tempPriority[i] = tempPriority[pos]
		tempPriority[pos] = temp

		temp = tempB[i]
		tempB[i] = tempB[pos]
		tempB[pos] = temp


		temp = tempPID[i]
		tempPID[i] = tempPID[pos]
		tempPID[pos] = temp

		temp = tempArr[i]
		tempArr[i] = tempArr[pos]
		tempArr[pos] = temp	

		
		i++
	}

	waiting_time[0] = 0
	i = 1
	for i < int64(n){
		waiting_time[i] = 0
		j = 0

		for j < i{
			
			waiting_time[i] = waiting_time[j] + tempB[j] - tempArr[i]
			j++
		}

		total = total + waiting_time[i]
		i++

	}

	
	total = 0
	i = 0

	for i < int64(n){
		tat[i] = tempB[i] + waiting_time[i]
		total = total + tat[i]
		i++
	}

	for i:= range processes{
		start := waiting_time[i] + tempArr[i]

		totalTurnaround += float64(tat[i])
		
		tempCompletion := tempB[i] + tempArr[i] + waiting_time[i]
		totalWaitTime += float64(waiting_time[i])

		if finalCompletion < tempCompletion{
			finalCompletion = tempCompletion
		}
	
		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  finalCompletion,
		})


	schedule[i] = []string{
		fmt.Sprint(tempPID[i]),
		fmt.Sprint(tempPriority[i]),
		fmt.Sprint(tempB[i]),
		fmt.Sprint(tempArr[i]),
		fmt.Sprint(waiting_time[i]),
		fmt.Sprint(tat[i]),
		fmt.Sprint(tempCompletion),

	}
}
avgTurnAround := totalTurnaround / float64(n)
avgWaitTime := totalWaitTime / float64(n)
avgThroughput := float64(n) / float64(finalCompletion)

outputTitle(w, title)
outputGantt(w, gantt)
outputSchedule(w, schedule, avgWaitTime, avgTurnAround, avgThroughput)
}

//
func SJFSchedule(w io.Writer, title string, processes []Process) { 
	
	var(
		x				=len(processes)
		rt				=make([]int64, x)
		waitingTime		=make([]int64, x)
		complete 		= 0
		t 				int64 
		minm			int64
		shortest 		int64
		finishTime		int64
		totalTurnaround	float64
		totalWaitTime	float64
		finalCompletion	int64
		check 			=false
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	
	minm =int64(math.MaxInt)

	for i := range processes{
		rt[i] = processes[i].BurstDuration

	}

	for complete < x{
		
		
			for i := range processes{
			if processes[i].ArrivalTime<= t	&& rt[i] < minm && rt[i] > 0{
				minm = rt[i] 
				shortest = int64(i)
				check = true
			}
		}

		if check == false{
			t++
			continue
		}

		
		rt[shortest]--

		minm = rt[shortest]
		if minm == 0{
			minm = int64(math.MaxInt)
		}

		if rt[shortest] == 0{
			complete++
			check = false

			finishTime = t+1

			waitingTime[shortest] = finishTime - processes[shortest].BurstDuration - processes[shortest].ArrivalTime

			if waitingTime[shortest] < 0{
				waitingTime[shortest] = 0
			}
			
		}
		t++
		
	}
			

		for i:= range processes{
			start := waitingTime[i] + processes[i].ArrivalTime
			
			turnaround := findTurnAroundTime(processes, waitingTime)
			totalTurnaround += float64(turnaround[i])
			
			tempCompletion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime[i]
			totalWaitTime += float64(waitingTime[i])

			if finalCompletion < tempCompletion{
				finalCompletion = tempCompletion
			}
		
			gantt = append(gantt, TimeSlice{
				PID:   processes[i].ProcessID,
				Start: start,
				Stop:  finalCompletion,
			})


		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime[i]),
			fmt.Sprint(turnaround[i]),
			fmt.Sprint(tempCompletion),

		}
	}
	avgTurnAround := totalTurnaround / float64(x)
	avgWaitTime := totalWaitTime / float64(x)
	avgThroughput := float64(x) / float64(finalCompletion)
	
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, avgWaitTime, avgTurnAround, avgThroughput)
}



func findTurnAroundTime(processes []Process, waitingTime[]int64)[]int64{
	tat := make([]int64, len(processes))

	for i := range processes{
		tat[i] = processes[i].BurstDuration + waitingTime[i]
	}
	
	return tat
}

//
func RRSchedule(w io.Writer, title string, processes []Process) { 
	var(
		count			int
		n				=len(processes)
		time 			int
		remain			int
		flag       		= 0
		time_quantum 	= 2 //manually change to test other quantum values
		wait_time		= make([]int64, n)
		turnaround_time	=make([]int64, n)
		rt				=make([]int64, n)

		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)

		totalTurnaround	float64
		totalWaitTime	float64
		finalCompletion	int64

	)
	

	
	remain = n
	count = 0
	for count < n {
		rt[count] = processes[count].BurstDuration		
		count++
	}
	
	time = 0 
	count = 0
	for remain != 0 {
		if rt[count] <= int64(time_quantum) && 0 < rt[count] {
			time = time + int(rt[count])
			rt[count] = 0
			flag = 1
		} else {
			if 0 < rt[count] {
				rt[count] = rt[count] - int64(time_quantum)
				time = time + time_quantum
			}
		}
		if rt[count] == 0 && flag == 1 {
			remain--
			wait_time[count] = wait_time[count] + int64(time) - int64(processes[count].ArrivalTime) - int64(processes[count].BurstDuration)
			turnaround_time[count] = turnaround_time[count] + int64(time) - int64(processes[count].ArrivalTime)
			flag = 0
		}
		if count == n - 1 {
			count = 0
		} else {
			if int(processes[count + 1].ArrivalTime) <= time {
				count++
			} else {
				count = 0
			}
		}
	
	}

	for i:= range processes{
		start := wait_time[i] + processes[i].ArrivalTime
		
		totalTurnaround += float64(turnaround_time[i])
		
		tempCompletion := processes[i].BurstDuration + processes[i].ArrivalTime + wait_time[i]
		totalWaitTime += float64(wait_time[i])

		if finalCompletion < tempCompletion{
			finalCompletion = tempCompletion
		}
	
		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  finalCompletion,
		})


	schedule[i] = []string{
		fmt.Sprint(processes[i].ProcessID),
		fmt.Sprint(processes[i].Priority),
		fmt.Sprint(processes[i].BurstDuration),
		fmt.Sprint(processes[i].ArrivalTime),
		fmt.Sprint(wait_time[i]),
		fmt.Sprint(turnaround_time[i]),
		fmt.Sprint(tempCompletion),

	}
}
avgTurnAround := totalTurnaround / float64(n)
avgWaitTime := totalWaitTime / float64(n)
avgThroughput := float64(n) / float64(finalCompletion)

outputTitle(w, title)
outputGantt(w, gantt)
outputSchedule(w, schedule, avgWaitTime, avgTurnAround, avgThroughput)


}


	

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
