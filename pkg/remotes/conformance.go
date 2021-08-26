package remotes

import (
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/remotes"
)

var (
	_ remotes.Resolver = (*pipeResolver)(nil)
	_ content.Writer   = (*pipeWriter)(nil)
)
