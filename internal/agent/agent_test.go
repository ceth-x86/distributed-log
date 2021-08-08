package agent

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	api "distributed/api/log.v1"

	"google.golang.org/grpc"

	"github.com/stretchr/testify/require"

	"github.com/travisjeffery/go-dynaport"
)

func TestAgent(t *testing.T) {
	var agents []*Agent
	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := ioutil.TempDir("", "server-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}
		agent, err := New(Config{
			NodeName:       fmt.Sprintf("%d", i),
			StartJoinAddrs: startJoinAddrs,
			BindAddr:       bindAddr,
			RPCPort:        rpcPort,
			DataDir:        dataDir,
		})
		require.NoError(t, err)
		agents = append(agents, agent)
	}

	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()
	time.Sleep(3 * time.Second)

	leaderClient := client(t, agents[0])
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("foo"),
			},
		})
	require.NoError(t, err)
	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		})
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))

	// wait until replication has finished
	time.Sleep(3 * time.Second)
	followerClient := client(t, agents[1])
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		})
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))
}

func client(t *testing.T,
	agent *Agent,
) api.LogClient {
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)
	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	conn, err := grpc.Dial(fmt.Sprintf("%s", rpcAddr), clientOptions...)
	require.NoError(t, err)
	client := api.NewLogClient(conn)
	return client
}
