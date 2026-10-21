package chunk_test

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// blockingChunkReader serves chunks from a map and lets tests observe
// how many fetches are in flight at once.
type blockingChunkReader struct {
	data         map[digest.Digest][]byte
	failing      map[digest.Digest]error
	release      chan struct{}
	inFlight     atomic.Int32
	maxInFlight  atomic.Int32
	mu           sync.Mutex
	fetchedOrder []digest.Digest
}

func (r *blockingChunkReader) Read(ctx context.Context, d digest.Digest) ([]byte, error) {
	n := r.inFlight.Add(1)
	for {
		m := r.maxInFlight.Load()
		if n <= m || r.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	defer r.inFlight.Add(-1)
	r.mu.Lock()
	r.fetchedOrder = append(r.fetchedOrder, d)
	r.mu.Unlock()
	if r.release != nil {
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err, ok := r.failing[d]; ok {
		return nil, err
	}
	return r.data[d], nil
}

func newTestChunks(t *testing.T, count int) ([]digest.Digest, map[digest.Digest][]byte, []byte) {
	t.Helper()
	digestFunction := digest.MustNewFunction("hello", remoteexecution.DigestFunction_SHA256)
	digests := make([]digest.Digest, 0, count)
	data := map[digest.Digest][]byte{}
	var whole []byte
	for i := 0; i < count; i++ {
		chunkData := []byte{byte(i), byte(i >> 8), 'x', 'y'}
		generator := digestFunction.NewGenerator(int64(len(chunkData)))
		_, err := generator.Write(chunkData)
		require.NoError(t, err)
		d := generator.Sum()
		digests = append(digests, d)
		data[d] = chunkData
		whole = append(whole, chunkData...)
	}
	return digests, data, whole
}

func TestPrefetchingReaderFromDigestsPreservesOrder(t *testing.T) {
	digests, data, whole := newTestChunks(t, 100)
	reader := &blockingChunkReader{data: data}

	got, err := io.ReadAll(chunk.NewPrefetchingReaderFromDigests(context.Background(), digests, reader, 8))
	require.NoError(t, err)
	require.Equal(t, whole, got)
	require.LessOrEqual(t, reader.maxInFlight.Load(), int32(8))
}

func TestPrefetchingReaderFromDigestsFetchesAhead(t *testing.T) {
	digests, data, whole := newTestChunks(t, 20)
	release := make(chan struct{})
	reader := &blockingChunkReader{data: data, release: release}

	r := chunk.NewPrefetchingReaderFromDigests(context.Background(), digests, reader, 4)
	// Nothing has been consumed yet, but fetches should already be
	// in flight, bounded by the requested concurrency.
	require.Eventually(t, func() bool { return reader.inFlight.Load() == 4 }, 5e9, 1e6)
	close(release)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, whole, got)
	require.Equal(t, int32(4), reader.maxInFlight.Load())
}

func TestPrefetchingReaderFromDigestsPropagatesErrors(t *testing.T) {
	digests, data, _ := newTestChunks(t, 10)
	reader := &blockingChunkReader{
		data:    data,
		failing: map[digest.Digest]error{digests[3]: status.Error(codes.NotFound, "Chunk not found")},
	}

	_, err := io.ReadAll(chunk.NewPrefetchingReaderFromDigests(context.Background(), digests, reader, 2))
	testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Failed to fetch chunk at index 3: Chunk not found"), err)
}

func TestPrefetchingReaderFromDigestsEmpty(t *testing.T) {
	got, err := io.ReadAll(chunk.NewPrefetchingReaderFromDigests(context.Background(), nil, &blockingChunkReader{}, 4))
	require.NoError(t, err)
	require.Empty(t, got)
}
