package node

import (
	"context"
	"crypto/rand"
	mockda "github.com/celestiaorg/optimint/da/mock"
	"github.com/celestiaorg/optimint/p2p"
	"github.com/stretchr/testify/assert"
	mrand "math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/proxy"
	"github.com/tendermint/tendermint/types"

	"github.com/celestiaorg/optimint/config"
	"github.com/celestiaorg/optimint/da"
	"github.com/celestiaorg/optimint/mocks"
)

func TestAggregatorMode(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	app := &mocks.Application{}
	app.On("CheckTx", mock.Anything).Return(abci.ResponseCheckTx{})
	app.On("BeginBlock", mock.Anything).Return(abci.ResponseBeginBlock{})
	app.On("DeliverTx", mock.Anything).Return(abci.ResponseDeliverTx{})
	app.On("EndBlock", mock.Anything).Return(abci.ResponseEndBlock{})
	app.On("Commit", mock.Anything).Return(abci.ResponseCommit{})

	key, _, _ := crypto.GenerateEd25519Key(rand.Reader)
	anotherKey, _, _ := crypto.GenerateEd25519Key(rand.Reader)

	blockManagerConfig := config.BlockManagerConfig{
		BlockTime:   500 * time.Millisecond,
		NamespaceID: [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	node, err := NewNode(context.Background(), config.NodeConfig{DALayer: "mock", Aggregator: true, BlockManagerConfig: blockManagerConfig}, key, proxy.NewLocalClientCreator(app), &types.GenesisDoc{ChainID: "test"}, log.TestingLogger())
	require.NoError(err)
	require.NotNil(node)

	assert.False(node.IsRunning())

	err = node.Start()
	assert.NoError(err)
	defer func() {
		err := node.Stop()
		assert.NoError(err)
	}()
	assert.True(node.IsRunning())

	pid, err := peer.IDFromPrivateKey(anotherKey)
	require.NoError(err)
	ctx, cancel := context.WithCancel(context.TODO())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				node.incomingTxCh <- &p2p.GossipMessage{Data: []byte(time.Now().String()), From: pid}
				time.Sleep(time.Duration(mrand.Uint32()%20) * time.Millisecond)
			}
		}
	}()
	time.Sleep(5 * time.Second)
	cancel()
}

// TestTxGossipingAndAggregation setups a network of nodes, with single aggregator and multiple producers.
// Nodes should gossip transactions and aggregator node should produce blocks.
func TestTxGossipingAndAggregation(t *testing.T) {
	require := require.New(t)

	nodes, aggApp := createNodes(11, t)

	for _, n := range nodes {
		require.NoError(n.Start())
	}

	time.Sleep(1 * time.Second)

	for i := 1; i < len(nodes); i++ {
		data := strconv.Itoa(i) + time.Now().String()
		require.NoError(nodes[i].P2P.GossipTx(context.TODO(), []byte(data)))
	}

	time.Sleep(4 * time.Second)

	for _, n := range nodes {
		require.NoError(n.Stop())
	}

	aggApp.AssertNumberOfCalls(t, "DeliverTx", 10)
	aggApp.AssertExpectations(t)
}

func createNodes(num int, t *testing.T) ([]*Node, *mocks.Application) {
	t.Helper()

	// create keys first, as they are required for P2P connections
	keys := make([]crypto.PrivKey, num)
	for i := 0; i < num; i++ {
		keys[i], _, _ = crypto.GenerateEd25519Key(rand.Reader)
	}

	nodes := make([]*Node, num)
	var aggApp *mocks.Application
	dalc := &mockda.MockDataAvailabilityLayerClient{}
	_ = dalc.Init(nil, nil, log.TestingLogger())
	_ = dalc.Start()
	nodes[0], aggApp = createNode(0, true, dalc, keys, t)
	for i := 1; i < num; i++ {
		nodes[i], _ = createNode(i, false, dalc, keys, t)
	}

	return nodes, aggApp
}

func createNode(n int, aggregator bool, dalc da.DataAvailabilityLayerClient, keys []crypto.PrivKey, t *testing.T) (*Node, *mocks.Application) {
	t.Helper()
	require := require.New(t)
	// nodes will listen on consecutive ports on local interface
	// random connections to other nodes will be added
	startPort := 10000
	p2pConfig := config.P2PConfig{
		ListenAddress: "/ip4/127.0.0.1/tcp/" + strconv.Itoa(startPort+n),
	}
	bmConfig := config.BlockManagerConfig{
		BlockTime:   200 * time.Millisecond,
		NamespaceID: [8]byte{8, 7, 6, 5, 4, 3, 2, 1},
	}
	for i := 0; i < len(keys); i++ {
		if i == n {
			continue
		}
		r := i
		id, err := peer.IDFromPrivateKey(keys[r])
		require.NoError(err)
		p2pConfig.Seeds += "/ip4/127.0.0.1/tcp/" + strconv.Itoa(startPort+r) + "/p2p/" + id.Pretty() + ","
	}
	p2pConfig.Seeds = strings.TrimSuffix(p2pConfig.Seeds, ",")

	app := &mocks.Application{}
	app.On("CheckTx", mock.Anything).Return(abci.ResponseCheckTx{})
	app.On("BeginBlock", mock.Anything).Return(abci.ResponseBeginBlock{})
	app.On("EndBlock", mock.Anything).Return(abci.ResponseEndBlock{})
	app.On("Commit", mock.Anything).Return(abci.ResponseCommit{})
	app.On("DeliverTx", mock.Anything).Return(abci.ResponseDeliverTx{})

	node, err := NewNode(
		context.Background(),
		config.NodeConfig{
			P2P:                p2pConfig,
			DALayer:            "mock",
			Aggregator:         aggregator,
			BlockManagerConfig: bmConfig,
		},
		keys[n],
		proxy.NewLocalClientCreator(app),
		&types.GenesisDoc{ChainID: "test"},
		log.TestingLogger().With("node", n))
	require.NoError(err)
	require.NotNil(node)

	// use same, common DALC, so nodes can share data
	node.dalc = dalc
	node.blockManager.SetDALC(dalc)

	return node, app
}
