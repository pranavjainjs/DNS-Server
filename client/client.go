package client

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"time"

	pb "Code/proto/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Request struct {
	RequestType int    // 0 for Read, 1 for Write
	Key         string // Key
	Value       string // Value
	Modified    bool   // Modified is true when the operation is successful
}

type Client struct {
	ID        int
	View      int
	RequestID int
	Requests  []*Request
}

var (
	addr = "localhost:5000"
)

func (client *Client) Run() {
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

	// REQUESTS FROM MAIN.GO

	for i := 0; i < len(client.Requests); i++ {
		randomInt := rand.Intn(2) + 1
		time.Sleep(time.Duration(randomInt) * time.Second)
		if client.Requests[i].RequestType == 0 {
			log.Printf("Client %d: Sent request, RequestID: %d, asking to read the IP address of %s\n", client.ID, client.RequestID, client.Requests[i].Key)
			r, err := c.Read(ctx, &pb.ReadRequest{Requestid: int64(client.RequestID), Clientid: int64(client.ID), Websitename: client.Requests[i].Key})
			if err != nil {
				log.Printf("Client %d: could not greet: %v", client.ID, err)
				// time.Sleep(5*time.Second)
				// client.View++;/
				break
			}
			log.Printf("Client %d: Received Reply: %s\n", client.ID, r.GetValue())
		} else {
			log.Printf("Client %d: Sent request, RequestID: %d, asking to modify the IP address of %s to %s\n", client.ID, client.RequestID, client.Requests[i].Key, client.Requests[i].Value)
			r, err := c.Write(ctx, &pb.WriteRequest{Requestid: int64(client.RequestID), Clientid: int64(client.ID), Websitename: client.Requests[i].Key, Address: client.Requests[i].Value})
			if err != nil {
				log.Printf("Client %d: could not greet: %v", client.ID, err)
				// time.Sleep(5*time.Second)
				// client.View++;
				break
			}
			log.Printf("Client %d: Received Reply: %v\n", client.ID, r.GetModified())
		}
		client.RequestID++
	}

	log.Printf("Client %d completed", client.ID)
}
