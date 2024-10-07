package replica

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	pb "Code/proto/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Replica struct {
	ID                   int
	Address              string
	DNS                  map[string]string
	Log                  []* LogEntry
	View                 int
	IsPrimary            bool
	ViewChangeInProgress bool
	State                int
	CurLogID             int
	WaitingLogs 		 map[int]* LogEntry
}

type LogEntry struct {
	LogID     int
	RequestID int
	ClientID  int
	Operation int
	Key       string
	Value     string
	Committed bool
}

type server struct {
	pb.UnimplementedReplicaServerServer
	rep         *Replica
	replicaSize int
	f           int
}

func (s *server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadReply, error) {
	log.Printf("Replica %d: Received Read Request from Client: %d, with request ID: %d and Website Name: %s\n", s.rep.ID, req.GetClientid(), req.GetRequestid(), req.GetWebsitename())
	ip, exists := s.rep.DNS[req.GetWebsitename()]
	if exists {
		return &pb.ReadReply{Value: ip}, nil
	}
	return &pb.ReadReply{Value: "NA"}, nil
}

func (s *server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteReply, error) {
	log.Printf("Replica %d: Received Write Request from Client: %d, with request: %d, Website Name: %s and New Address: %s\n", s.rep.ID, req.GetClientid(), req.GetRequestid(), req.GetWebsitename(), req.GetAddress())
	_, exists := s.rep.DNS[req.GetWebsitename()]
	if exists {
		logentry := &LogEntry{LogID: s.rep.CurLogID, RequestID: int(req.GetRequestid()), ClientID: int(req.GetClientid()), Operation: 1, Key: req.GetWebsitename(), Value: req.GetAddress(), Committed: false}
		s.rep.WaitingLogs[s.rep.CurLogID] = logentry
		s.broadcastVerify(int64(s.rep.CurLogID), req.GetRequestid(), req.GetClientid(), req.GetWebsitename(), req.GetAddress())
		s.rep.Log = append(s.rep.Log, logentry)
		s.rep.DNS[logentry.Key] = s.rep.DNS[logentry.Value]
		delete(s.rep.WaitingLogs, s.rep.CurLogID)
		log.Println("Replica 1: Received f number of OKs from the replicas!")
		return &pb.WriteReply{Modified: true}, nil
	}
	return &pb.WriteReply{Modified: false}, fmt.Errorf("website doesn't exist in the DNS. please check and try again")
}

func (s *server) broadcastVerify(logid, requestID, clientID int64, websiteName, address string) {
	var wg sync.WaitGroup
	doneCh := make(chan bool, s.replicaSize) 
	threshold := s.f - 1

	for i := 1; i < s.replicaSize; i++ {
		wg.Add(1)
		go func(replicaID int) {
			defer wg.Done()
			conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", 5000+replicaID), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Replica %d: Error connecting to Replica %d", s.rep.ID, replicaID)
				return
			}
			defer conn.Close()

			client := pb.NewReplicaServerClient(conn)
			_, err = client.Prepare(context.Background(), &pb.PrepareRequest{
				Logid:       logid,
				Requestid:   requestID,
				Clientid:    clientID,
				Websitename: websiteName,
				Address:     address,
			})
			if err == nil {
				log.Printf("Replica %d: Sent Prepare to Replica %d", s.rep.ID, replicaID)
				doneCh <- true 
			} else {
				log.Printf("Replica %d: Failed to send Prepare to Replica %d", s.rep.ID, replicaID)
			}
		}(i)
	}

	for i := 0; i < threshold; i++ {
		select {
		case <-doneCh:
			log.Println("Received one OK!")
		case <-time.After(5 * time.Second): 
			log.Println("Timeout waiting for response")
			return
		}
	}

	wg.Wait()
}


func (s *server) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareOKReply, error) {
	log.Printf("Replica %d: Received Prepare Request from Replica %d", s.rep.ID, 0)
	logentry := &LogEntry{LogID: int(req.GetLogid()), RequestID: int(req.GetRequestid()), ClientID: int(req.GetClientid()), Operation: 1, Key: req.GetWebsitename(), Value: req.GetAddress(), Committed: false}
	s.rep.WaitingLogs[logentry.LogID] = logentry
	log.Printf("Replica %d: Sent PrepareOK Reply to Replica %d", s.rep.ID, 0)
	return &pb.PrepareOKReply{Received: true}, nil
}

func (rep *Replica) PrintDetails() {
	fmt.Printf("ID: %d\n", rep.ID)
	fmt.Printf("Address: %s\n", rep.Address)
	fmt.Println("DNS:")
	for key, value := range rep.DNS {
		fmt.Printf("Key: %s, Value: %s\n", key, value)
	}
	fmt.Printf("View: %d\n", rep.View)
	fmt.Printf("IsPrimary: %v\n", rep.IsPrimary)
	fmt.Printf("State: %d\n", rep.State)
}

func (rep *Replica) Run(wg *sync.WaitGroup, port int, replicaSize int) {
	defer wg.Done()

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := &server{rep: rep, replicaSize: replicaSize, f: replicaSize/2 + 1}
	grpcServer := grpc.NewServer()
	pb.RegisterReplicaServerServer(grpcServer, s)

	timeout := 40 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	go func() {
		log.Printf("gRPC server listening on port: %d", port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	<-ctx.Done()

	grpcServer.GracefulStop()
	log.Printf("gRPC server listening on port: %d stopped gracefully", port)
}
