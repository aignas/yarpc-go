package peer_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/yarpc/api/peer"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/peer/circus"
	"go.uber.org/yarpc/peer/hostport"
	"go.uber.org/yarpc/peer/pendingheap"
	"go.uber.org/yarpc/peer/randpeer"
	"go.uber.org/yarpc/peer/roundrobin"
	"go.uber.org/yarpc/peer/tworandomchoices"
	"go.uber.org/yarpc/yarpctest"
)

// This benchmark assesses the speed of choose + finish for each of the YARPC
// peer choosers, with varying numbers of peers, and varying latencies.
// In order to remove exogenous behaviors from the simulation, each new request
// may be finished any number of turns in the future.
// One random request will be finished for every new peer chosen.
// By increasing the size of the concurrency window, we can increase the
// variance of pending requests on each individual peer.

// Size is the number of peers in the list.
// For this benchmark, the number of peers is fixed.
// All peers are connected.
// Variance is the number of concurrent pending requests.
// this number is also fixed for the benchmark.
// Variance comes into play because the request that finishes is chosen
// randomly from the window of previously chosen peers.
func bench(t *testing.B, size int, variance int, newList func(peer.Transport) peer.ChooserList) {
	t.ReportAllocs()

	trans := yarpctest.NewFakeTransport()
	list := newList(trans)

	// Build a static membership for the list.
	var ids []peer.Identifier
	for i := 0; i < size; i++ {
		ids = append(ids, hostport.Identify(strconv.Itoa(i)))
	}
	err := list.Update(peer.ListUpdates{
		Additions: ids,
	})
	require.NoError(t, err)

	trans.Start()
	defer trans.Stop()
	list.Start()
	defer list.Stop()

	req := &transport.Request{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build a bank of concurrent requests.
	finishers := make([]func(error), variance)
	{
		i := 0
		for {
			_, onFinish, _ := list.Choose(ctx, req)
			if onFinish != nil {
				finishers[i] = onFinish
				i++
				if i >= variance {
					break
				}
			}
		}
	}

	t.ResetTimer()
	for n := 0; n < t.N; n++ {
		_, onFinish, _ := list.Choose(ctx, req)
		if onFinish == nil {
			continue
		}
		index := rand.Intn(variance)
		finishers[index](nil)
		finishers[index] = onFinish
	}
}

func newCircus(trans peer.Transport) peer.ChooserList { return circus.New(trans) }
func newHeap(trans peer.Transport) peer.ChooserList   { return pendingheap.New(trans) }
func newTRC(trans peer.Transport) peer.ChooserList    { return tworandomchoices.New(trans) }
func newRR(trans peer.Transport) peer.ChooserList     { return roundrobin.New(trans) }
func newRandom(trans peer.Transport) peer.ChooserList { return randpeer.New(trans) }

func Benchmark(t *testing.B) {
	listFuncs := []struct {
		name    string
		newList func(peer.Transport) peer.ChooserList
	}{
		{"roundrobin", newRR},
		{"random", newRandom},
		{"tworandom", newTRC},
		{"heap", newHeap},
		{"circus", newCircus},
	}

	variances := []int{
		1,
		256,
		1024,
	}

	sizes := []int{
		1,
		2,
		4,
		8,
		16,
		32,
		64,
		128,
		256,
		512,
		1024,
	}

	for _, lf := range listFuncs {
		for _, size := range sizes {
			for _, variance := range variances {
				t.Run(fmt.Sprintf("%s-%dpeers-%dvariance", lf.name, size, variance), func(t *testing.B) {
					bench(t, size, variance, lf.newList)
				})
			}
		}
	}
}
