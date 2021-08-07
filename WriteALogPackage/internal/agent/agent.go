package agent

import (
	"distributed/WriteALogPackage/internal/discovery"
	"distributed/WriteALogPackage/internal/log"
	"distributed/WriteALogPackage/internal/server"
	"fmt"
	"net"
	"sync"

	api "distributed/WriteALogPackage/api/log.v1"

	"google.golang.org/grpc"
)

type Config struct {
	DataDir        string
	BindAddr       string
	RPCPort        int
	NodeName       string
	StartJoinAddrs []string
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

type Agent struct {
	Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(
		a.Config.DataDir,
		log.Config{},
	)
	return err
}

func (a *Agent) setupServer() error {
	serverConfig := &server.Config{
		CommitLog: *a.log,
	}
	var err error
	a.server, err = server.NewGRPCServer(serverConfig)
	if err != nil {
		return err
	}
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	go func() {
		if err := a.server.Serve(ln); err != nil {
			_ = a.Shutdown()
		}
	}()
	return err
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	conn, err := grpc.Dial(rpcAddr, clientOptions...)
	if err != nil {
		return err
	}
	client := api.NewLogClient(conn)
	a.replicator = &log.Replicator{
		LocalServer: client,
	}
	a.membership, err = discovery.New(a.replicator, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartToJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
