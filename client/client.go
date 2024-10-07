package client

import (
	"context"
	"flag"
	"log"
	"time"
	"math/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "Code/proto/proto"
)

type Client struct {
	ID int
	View int
	RequestID int
}

var (
	addr = "localhost:5000"
)

func (client * Client) Run () {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewReplicaServerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	// requests := []string{"Google", "Baidu", "X"};
	// for i := 0; i < 3; i++ {
	// 	randomInt := rand.Intn(2) + 1 
	// 	time.Sleep(time.Duration(randomInt)*time.Second)
	// 	log.Printf("Client %d: Sent request, RequestID: %d, asking for the IP address of %s\n", client.ID, client.RequestID, requests[i])
	// 	r, err := c.Read(ctx, &pb.ReadRequest{Requestid: int64(client.RequestID), Clientid: int64(client.ID), Websitename: requests[i]})
	// 	if err != nil {
	// 		log.Fatalf("could not greet: %v", err)
	// 	}
	// 	log.Printf("Client %d: Received IP Address: %s\n", client.ID, r.GetValue())
	// 	client.RequestID++
	// }

	requests := []string{"Google"};
	for i := 0; i < 1; i++ {
		randomInt := rand.Intn(2) + 1 
		time.Sleep(time.Duration(randomInt)*time.Second)
		log.Printf("Client %d: Sent request, RequestID: %d, asking to modify the IP address of %s to %s\n", client.ID, client.RequestID, requests[i], "127.0.0.sanjana")
		r, err := c.Write(ctx, &pb.WriteRequest{Requestid: int64(client.RequestID), Clientid: int64(client.ID), Websitename: requests[i], Address: "127.0.0.sanjana"})
		if err != nil {
			log.Fatalf("could not greet: %v", err)
		}
		log.Printf("Client %d: Received Reply: %v\n", client.ID, r.GetModified())
		client.RequestID++
	}
	log.Printf("Client %d completed", client.ID)
}