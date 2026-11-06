package main

import (
	"context"
	"os"

	"github.com/pipelines-as-code/tkn-local/internal/command"
	"github.com/pipelines-as-code/tkn-local/internal/iostreams"
)

func main() {
	streams := &iostreams.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}
	if err := command.New(streams).ExecuteContext(context.Background()); err != nil {
		os.Exit(1)
	}
}
