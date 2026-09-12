package main

import (
	"os"

	"github.com/vestavision/trail"
	trstdout "github.com/vestavision/trail/sink/stdout"
)

func main() {
	if err := trail.Init(trail.Config{
		Service:     "order-worker",
		Environment: "development",
		Sink:        trstdout.New(os.Stdout),
	}); err != nil {
		panic(err)
	}
	defer trail.Close() // Production programs may inspect the returned error.

	trail.Log("service.started", trail.String("component", "matcher"))
}
