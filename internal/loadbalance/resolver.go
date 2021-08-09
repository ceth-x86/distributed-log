package loadbalance

import (
	"context"
	"fmt"
	"log"
	"sync"

	"google.golang.org/grpc/attributes"

	api "distributed/api/log.v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
}

var _ resolver.Builder = (*Resolver)(nil)
var _ resolver.Resolver = (*Resolver)(nil)

func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(dialOpts,
			grpc.WithTransportCredentials(opts.DialCreds))
	} else {
		dialOpts = []grpc.DialOption{grpc.WithInsecure()}
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingconfig": [{%s:{}}]}`, Name))
	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

func (r *Resolver) Scheme() string {
	return Name
}

func init() {
	resolver.Register(&Resolver{})
}

const Name = "proglog"

func (r *Resolver) ResolveNow(options resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	client := api.NewLogClient(r.resolverConn)

	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		log.Printf("[ERROR] proglog: failed to resolve servers: %v", err)
		return
	}
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr:       server.RpcAddr,
			Attributes: attributes.New("is_leader", server.IsLeader),
		})
	}
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		log.Printf("[ERROR] proglog: failed to close conn: %v", err)
	}
}
