package main

import (
	client "Code/client"
	replica "Code/replica"
	"fmt"
	"log"
	"strconv"
	"sync"
)

func InitializeDNS (rep *replica.Replica) {
    rep.DNS = make(map[string]string)
	rep.DNS["Google"] = "127.0.0.0"
	rep.DNS["YouTube"] = "127.0.0.1"
	rep.DNS["Facebook"] = "127.0.0.2"
	rep.DNS["X"] = "127.0.0.3"
	rep.DNS["Wikipedia"] = "127.0.0.4"
	rep.DNS["Baidu"] = "127.0.0.5"
	rep.DNS["TikTo"] = "127.0.0.6"
	rep.DNS["Amazon"] = "127.0.0.7"
	rep.DNS["Yahoo"] = "127.0.0.8"
}


func Initialize(rep *replica.Replica, port int) {
    rep.ID = (port) % 5000
	rep.Address = "127.0.0." + strconv.Itoa(port)
	InitializeDNS(rep)
	rep.View = 0
	if rep.ID == 0 {
		rep.IsPrimary = true
	} else {
		rep.IsPrimary = false
	}
	rep.ViewChangeInProgress = false
	rep.State = 0
	rep.CurLogID = 1;
	rep.WaitingLogs = make(map[int]*replica.LogEntry)
}

func InitializeClient(client * client.Client, id int) {
	client.ID = id
	client.View = 0
	client.RequestID = 1
}

func main() {
	var replicaSize int
	var clientSize int
	var wg sync.WaitGroup

	fmt.Print("Enter the number of Replicas: ")
	_, err := fmt.Scan(&replicaSize)
	if err != nil {
		log.Fatalf("Error reading input: %v", err)
	}

	fmt.Print("Enter the number of clients: ")
	_, err = fmt.Scan(&clientSize)
	if err != nil {
		log.Fatalf("Error reading input: %v", err)
	}

	replicasArray := make([]replica.Replica, replicaSize)
	clientsArray := make([]client.Client, clientSize)

	for i := 0; i < replicaSize; i++ {
		Initialize(&replicasArray[i], 5000 + i)
		wg.Add(1)
		go replicasArray[i].Run(&wg, 5000 + i, replicaSize)
	}

	for i := 0; i < clientSize; i++ {
		InitializeClient(&clientsArray[i], i)
		go clientsArray[i].Run()
	}

	wg.Wait()
	fmt.Println("Successfull!!")
}