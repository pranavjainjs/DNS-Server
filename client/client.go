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

	// requests := []string{"Google"};
	// for i := 0; i < 1; i++ {
	// 	randomInt := rand.Intn(2) + 1 
	// 	time.Sleep(time.Duration(randomInt)*time.Second)
	// 	log.Printf("Client %d: Sent request, RequestID: %d, asking to modify the IP address of %s to %s\n", client.ID, client.RequestID, requests[i], "127.0.0.sanjana")
	// 	r, err := c.Write(ctx, &pb.WriteRequest{Requestid: int64(client.RequestID), Clientid: int64(client.ID), Websitename: requests[i], Address: "127.0.0.sanjana"})
	// 	if err != nil {
	// 		log.Fatalf("could not greet: %v", err)
	// 	}
	// 	log.Printf("Client %d: Received Reply: %v\n", client.ID, r.GetModified())
	// 	client.RequestID++
	// }
	// log.Printf("Client %d completed", client.ID)

	requests_odd_read := []string{"Google"}
    requests_odd_write := map[string]string{
        "Google": "127.0.0.Google", 
    }

    requests_even_read := []string{"YouTube"}
    requests_even_write := map[string]string{
        "Google": "127.0.0.Google", 
        "YouTube": "127.0.0.YouTube", 
    }

    var requests_read []string
    var requests_write map[string]string

    if client.ID%2 == 0 {
        requests_read = requests_even_read
        requests_write = requests_even_write
    } else {
        requests_read = requests_odd_read
        requests_write = requests_odd_write
    }

	for i := 0; i < 1; i++ {
		randomInt := rand.Intn(2) + 1 
		time.Sleep(time.Duration(randomInt)*time.Second)
		log.Printf("Client %d: Sent request, RequestID: %d, asking to read the IP address of %s\n", client.ID, client.RequestID, requests_read[i])
		r, err := c.Read(ctx, &pb.ReadRequest{Requestid: int64(client.RequestID), Clientid: int64(client.ID), Websitename: requests_read[i]})
		if err != nil {
			log.Fatalf("could not greet: %v", err)
		}
		log.Printf("Client %d: Received Reply: %s\n", client.ID, r.GetValue())
		client.RequestID++
	}

	for key, val := range requests_write {
		randomInt := rand.Intn(2) + 1 
		time.Sleep(time.Duration(randomInt)*time.Second)
		log.Printf("Client %d: Sent request, RequestID: %d, asking to modify the IP address of %s to %s\n", client.ID, client.RequestID, key, val)
		r, err := c.Write(ctx, &pb.WriteRequest{Requestid: int64(client.RequestID), Clientid: int64(client.ID), Websitename: key, Address: val})
		if err != nil {
			log.Fatalf("could not greet: %v", err)
		}
		log.Printf("Client %d: Received Reply: %v\n", client.ID, r.GetModified())
		client.RequestID++
	}

	if client.ID%2 == 1 {
		randomInt := rand.Intn(2) + 1 
		time.Sleep(time.Duration(randomInt)*time.Second)
		log.Printf("Client %d: Sent request, RequestID: %d, asking to modify the IP address of %s to %s\n", client.ID, client.RequestID, "Google", "127.0.0.Google")
		r, err := c.Write(ctx, &pb.WriteRequest{Requestid: int64(client.RequestID), Clientid: int64(client.ID), Websitename: "Google", Address: "127.0.0.Google"})
		if err != nil {
			log.Fatalf("could not greet: %v", err)
		}
		log.Printf("Client %d: Received Reply: %v\n", client.ID, r.GetModified())
		client.RequestID++
	}

	log.Printf("Client %d completed", client.ID)
}