package remotes

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/remotes"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Pipe given a source resolver, resolve the passed in ref to a desc
// return a fetcher and pusher, As you write to the pusher, the fetcher will receive the result
// Make sure you `go` to a different thread for writing, since writing will only happen after at least one read
func Pipe(ctx context.Context, source remotes.Resolver, ref string) (remotes.Fetcher, remotes.Pusher, error) {
	r := &pipeResolver{Resolver: source}

	f, e := r.Fetcher(ctx, ref)
	if e != nil {
		return nil, nil, e
	}

	p, e := r.Pusher(ctx, ref)
	if e != nil {
		return nil, nil, e
	}

	return f, p, nil
}

type pipeResolver struct {
	desc   ocispec.Descriptor
	writer *io.PipeWriter
	reader *io.PipeReader
	remotes.Resolver
}

type pipeWriter struct {
	desc   ocispec.Descriptor
	status content.Status
	writer *io.PipeWriter
	content.Writer
}

func (p *pipeResolver) Resolve(ctx context.Context, ref string) (string, ocispec.Descriptor, error) {
	return p.Resolver.Resolve(ctx, ref)
}

func (p *pipeResolver) Fetcher(ctx context.Context, ref string) (remotes.Fetcher, error) {
	from, to := io.Pipe()
	p.reader = from
	p.writer = to
	_, desc, err := p.Resolve(ctx, ref)
	if err != nil {
		return nil, err
	}

	p.desc = desc
	return p, nil
}

func (p *pipeResolver) Fetch(ctx context.Context, desc ocispec.Descriptor) (io.ReadCloser, error) {
	return p.reader, nil
}

func (p *pipeResolver) Pusher(ctx context.Context, ref string) (remotes.Pusher, error) {
	return &pipeWriter{
		desc:   ocispec.Descriptor{},
		status: content.Status{Ref: ref},
		writer: p.writer,
	}, nil
}

func (p *pipeWriter) Push(ctx context.Context, d ocispec.Descriptor) (content.Writer, error) {
	p.desc = d
	return p, nil
}

// Digest may return empty digest or panics until committed.
func (p *pipeWriter) Digest() digest.Digest {
	return p.desc.Digest
}

// Commit commits the blob (but no roll-back is guaranteed on an error).
// size and expected can be zero-value when unknown.
// Commit always closes the writer, even on error.
// ErrAlreadyExists aborts the writer.
func (p *pipeWriter) Commit(ctx context.Context, size int64, expected digest.Digest, opts ...content.Opt) error {
	var base content.Info
	for _, opt := range opts {
		if err := opt(&base); err != nil {
			return err
		}
	}

	p.desc.Size = size
	p.desc.Digest = expected
	p.desc.Annotations = base.Labels
	p.status.Expected = expected
	p.status.Total = size
	p.status.Offset = size
	p.writer.Close()

	return nil
}

// Status returns the current state of write
func (p *pipeWriter) Status() (content.Status, error) {
	return p.status, nil
}

// Truncate updates the size of the target blob
func (p *pipeWriter) Truncate(size int64) error {
	return fmt.Errorf("truncate is not enabled")
}

func (p *pipeWriter) Write(data []byte) (n int, err error) {
	n, err = p.writer.Write(data)
	if err != nil {
		p.status.Total += int64(n)
		p.status.Offset = p.status.Total
		p.status.UpdatedAt = time.Now()
		return n, err
	}

	p.status.Total += int64(n)
	p.status.Offset = p.status.Total
	p.status.UpdatedAt = time.Now()
	return n, err
}

func (p *pipeWriter) Close() error {
	return p.writer.Close()
}
