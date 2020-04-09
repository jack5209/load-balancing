package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/features/proto/echo"
)

const (
	serviceName = "g.srv.mail"
	ip          = "localhost:50053"
)

type ecServer struct {
	pb.UnimplementedEchoServer
}

func main() {
	s := grpc.NewServer()
	pb.RegisterEchoServer(s, &ecServer{})
	lis, err := net.Listen("tcp", ip)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// register
	go start()

	fmt.Println("server start.")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func start() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		log.Fatal(err)
	}

	key := serviceName + "/" + ip
	// val := json.Marshal(ip)

	// 创建一个租约
	resp, err := client.Grant(context.TODO(), 6)
	if err != nil {
		log.Fatal(err)
	}

	leaseID := resp.ID
	_, err = client.Put(context.TODO(), key, ip, clientv3.WithLease(leaseID))
	if err != nil {
		log.Fatal(err)
	}

	ch, err := client.KeepAlive(context.TODO(), leaseID)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-client.Ctx().Done():
			log.Fatal(errors.New("service closed"))
		case resp, ok := <-ch:
			// 监听租约
			if !ok {
				log.Println("keep alive channel closed")
				_, err := client.Revoke(context.TODO(), leaseID)
				if err != nil {
					log.Fatal(err)
				}

				log.Printf("service:%s stop\n", key)
			}

			log.Printf("Recv reply from service: %s, ttl:%d", key, resp.TTL)
		}
	}
}

func (s *ecServer) UnaryEcho(ctx context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	// fmt.Println("addr:", s.addr)
	return &pb.EchoResponse{Message: fmt.Sprintf("%s (from %s)", req.Message, ip)}, nil
}
