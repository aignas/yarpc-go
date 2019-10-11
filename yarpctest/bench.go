package yarpctest

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
	"go.uber.org/yarpc/peer/hostport"
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
func BenchmarkPeerListChooseFinish(t *testing.B, size int, variance int, newList func(peer.Transport) peer.ChooserList) {
	t.ReportAllocs()

	trans := NewFakeTransport()
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

func BenchmarkPeerListUpdate(t *testing.B, init peer.ConnectionStatus, newList func(peer.Transport) peer.ChooserList) {
	trans := NewFakeTransport(InitialConnectionStatus(init))
	list := newList(trans)
	rng := rand.NewSource(1)

	var oldBits int64

	t.ResetTimer()
	for n := 0; n < t.N; n++ {
		newBits := rng.Int63()
		additions := idsForBits(newBits &^ oldBits)
		removals := idsForBits(oldBits &^ newBits)
		err := list.Update(peer.ListUpdates{
			Additions: additions,
			Removals:  removals,
		})
		if err != nil {
			panic(fmt.Sprintf("benchmark invalidated by update error: %v", err))
		}
		oldBits = newBits
	}
}

func BenchmarkPeerListNotifyStatusChanged(t *testing.B, newList func(peer.Transport) peer.ChooserList) {
	trans := NewFakeTransport()
	list := newList(trans)
	rng := rand.NewSource(1)

	// Add all 63 peers.
	err := list.Update(peer.ListUpdates{
		Additions: bitIds[:],
	})
	if err != nil {
		panic(fmt.Sprintf("benchmark invalidated by update error: %v", err))
	}

	t.ResetTimer()
	// Divide N by numIds. Add one to round up from zero.
	for n := 0; n < t.N/numIds+1; n++ {
		bits := rng.Int63()
		for i := uint(0); i < numIds; i++ {
			bit := (1 << i) & bits
			if bit != 0 {
				trans.SimulateConnect(bitIds[i])
			} else {
				trans.SimulateDisconnect(bitIds[i])
			}
		}
	}
}
