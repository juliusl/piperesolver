package remotes

import (
	"context"

	"github.com/containerd/containerd/remotes"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type copier struct {
	desc   ocispec.Descriptor
	source pipeResolver
}

func (c copier) Copy(ctx context.Context, pusher remotes.Pusher) (ocispec.Descriptor, error) {

	return ocispec.Descriptor{}, nil
}
