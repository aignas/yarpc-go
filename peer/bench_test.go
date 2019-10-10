package peer_test

import (
	"context"
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

func newCircus(trans peer.Transport) peer.ChooserList {
	return circus.New(trans)
}

func newHeap(trans peer.Transport) peer.ChooserList {
	return pendingheap.New(trans)
}

func newTRC(trans peer.Transport) peer.ChooserList {
	return tworandomchoices.New(trans)
}

func newRR(trans peer.Transport) peer.ChooserList {
	return roundrobin.New(trans)
}

func newRandom(trans peer.Transport) peer.ChooserList {
	return randpeer.New(trans)
}

func BenchmarkCircus16(b *testing.B) { bench(b, 16, newCircus) }
func BenchmarkHeap16(b *testing.B)   { bench(b, 16, newHeap) }
func BenchmarkTRC16(b *testing.B)    { bench(b, 16, newTRC) }
func BenchmarkRR16(b *testing.B)     { bench(b, 16, newRR) }
func BenchmarkRandom16(b *testing.B) { bench(b, 16, newRandom) }

func BenchmarkCircus32(b *testing.B) { bench(b, 32, newCircus) }
func BenchmarkHeap32(b *testing.B)   { bench(b, 32, newHeap) }
func BenchmarkTRC32(b *testing.B)    { bench(b, 32, newTRC) }
func BenchmarkRR32(b *testing.B)     { bench(b, 32, newRR) }
func BenchmarkRandom32(b *testing.B) { bench(b, 32, newRandom) }

func BenchmarkCircus64(b *testing.B) { bench(b, 64, newCircus) }
func BenchmarkHeap64(b *testing.B)   { bench(b, 64, newHeap) }
func BenchmarkTRC64(b *testing.B)    { bench(b, 64, newTRC) }
func BenchmarkRR64(b *testing.B)     { bench(b, 64, newRR) }
func BenchmarkRandom64(b *testing.B) { bench(b, 64, newRandom) }

func BenchmarkCircus128(b *testing.B) { bench(b, 128, newCircus) }
func BenchmarkHeap128(b *testing.B)   { bench(b, 128, newHeap) }
func BenchmarkTRC128(b *testing.B)    { bench(b, 128, newTRC) }
func BenchmarkRR128(b *testing.B)     { bench(b, 128, newRR) }
func BenchmarkRandom128(b *testing.B) { bench(b, 128, newRandom) }

func bench(b *testing.B, size int, newList func(peer.Transport) peer.ChooserList) {
	b.ReportAllocs()

	trans := yarpctest.NewFakeTransport()
	list := newList(trans)

	var ids []peer.Identifier
	for i := 0; i < size; i++ {
		ids = append(ids, hostport.Identify(strconv.Itoa(i)))
	}

	err := list.Update(peer.ListUpdates{
		Additions: ids,
	})
	require.NoError(b, err)

	trans.Start()
	defer trans.Stop()
	list.Start()
	defer list.Stop()

	req := &transport.Request{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, onFinish, err := list.Choose(ctx, req)
		if err == nil {
			onFinish(nil)
		}
	}
}
