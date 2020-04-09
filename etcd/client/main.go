package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"

	ecpb "google.golang.org/grpc/examples/features/proto/echo"
	"google.golang.org/grpc/resolver"
)

const schema = "etcd"
const service = "g.srv.mail"

func main() {
	r := Resolver{}
	resolver.Register(&r)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	addr := fmt.Sprintf("%s:///%s", r.Scheme(), service)
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithInsecure(),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
		grpc.WithBlock())
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}

	client := ecpb.NewEchoClient(conn)

	// test load balance
	for i := 0; i < 10; i++ {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		resp, err := client.UnaryEcho(ctx, &ecpb.EchoRequest{Message: "hi jack."})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(resp.Message)
		time.Sleep(time.Second * 1)
	}
}

// Resolver ...
type Resolver struct{}

// Scheme ...
func (r *Resolver) Scheme() string {
	return schema + "/" + service
}

// Build ...
func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	})
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		// get addrs through service
		addrDict := make(map[string]resolver.Address)

		// context.Background() why ???
		resp, err := cli.Get(context.Background(), service, clientv3.WithPrefix())
		if err != nil {
			log.Fatal(err)
		}

		for i, kv := range resp.Kvs {
			fmt.Println("kv:", i, string(kv.Key), string(kv.Value))
			addrDict[string(kv.Key)] = resolver.Address{Addr: string(kv.Value)}
		}

		update := func() {
			addrList := make([]resolver.Address, len(addrDict))
			for _, v := range addrDict {
				addrList = append(addrList, v)
			}
			cc.NewAddress(addrList)
		}

		update()

		// watch
		rch := cli.Watch(context.Background(), service, clientv3.WithPrefix(), clientv3.WithPrevKV())
		for n := range rch {
			for _, ev := range n.Events {
				switch ev.Type {
				case mvccpb.PUT:
					fmt.Printf("mvccpb.PUT: %v\n", string(ev.Kv.Key))
					addrDict[string(ev.Kv.Key)] = resolver.Address{Addr: string(ev.Kv.Value)}
				case mvccpb.DELETE:
					delete(addrDict, string(ev.PrevKv.Key))
					fmt.Printf("mvccpb.DELETE: %v\n", string(ev.PrevKv.Key))
				}
			}
			update()
		}
	}()

	return r, nil
}

// ResolveNow ...
func (r *Resolver) ResolveNow(rn resolver.ResolveNowOptions) {

}

// Close ...
func (r *Resolver) Close() {

}
