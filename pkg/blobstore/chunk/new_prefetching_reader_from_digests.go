package chunk

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type prefetchedChunk struct {
	data []byte
	err  error
}

// prefetchingDigestsReader is an io.Reader that stitches together the
// contents of a blob from an ordered slice of chunk digests, fetching
// up to a bounded number of chunks ahead of the consumer.
type prefetchingDigestsReader struct {
	cancel  context.CancelFunc
	results <-chan <-chan prefetchedChunk

	currentChunkData   []byte
	currentChunkOffset int
	err                error
}

// NewPrefetchingReaderFromDigests creates an io.Reader that yields the
// concatenated contents of the chunks identified by the provided
// digests. Unlike NewReaderFromDigests, it keeps up to concurrency
// chunk fetches in flight, so that reassembling a blob is not bounded
// by the round-trip latency of a single chunk fetch.
func NewPrefetchingReaderFromDigests(ctx context.Context, chunkDigests []digest.Digest, chunkBytesReader reader.Reader[[]byte], concurrency int) io.Reader {
	if concurrency < 1 {
		concurrency = 1
	}
	ctx, cancel := context.WithCancel(ctx)
	results := make(chan (<-chan prefetchedChunk), concurrency)
	go func() {
		defer close(results)
		inFlight := make(chan struct{}, concurrency)
		for i, chunkDigest := range chunkDigests {
			select {
			case inFlight <- struct{}{}:
			case <-ctx.Done():
				return
			}
			result := make(chan prefetchedChunk, 1)
			go func(i int, chunkDigest digest.Digest) {
				data, err := chunkBytesReader.Read(ctx, chunkDigest)
				if err != nil {
					err = util.StatusWrapf(err, "Failed to fetch chunk at index %d", i)
				}
				result <- prefetchedChunk{data: data, err: err}
				<-inFlight
			}(i, chunkDigest)
			select {
			case results <- result:
			case <-ctx.Done():
				return
			}
		}
	}()
	return &prefetchingDigestsReader{
		cancel:  cancel,
		results: results,
	}
}

func (r *prefetchingDigestsReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.currentChunkOffset >= len(r.currentChunkData) {
		result, ok := <-r.results
		if !ok {
			r.err = io.EOF
			r.cancel()
			return 0, r.err
		}
		chunk := <-result
		if chunk.err != nil {
			r.err = chunk.err
			r.cancel()
			return 0, r.err
		}
		r.currentChunkData = chunk.data
		r.currentChunkOffset = 0
	}

	n := copy(p, r.currentChunkData[r.currentChunkOffset:])
	r.currentChunkOffset += n
	if r.currentChunkOffset >= len(r.currentChunkData) {
		r.currentChunkData = nil
	}
	return n, nil
}
