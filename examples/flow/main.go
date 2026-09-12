package main

import (
	"os"

	"github.com/vestavision/trail"
	trstdout "github.com/vestavision/trail/sink/stdout"
)

func main() {
	if err := trail.Init(trail.Config{Service: "order-worker", Sink: trstdout.New(os.Stdout)}); err != nil {
		panic(err)
	}
	defer trail.Close()

	executionID := trail.NewExecution()
	flowID := trail.NewFlow()
	trail.Log("catalog.sync.started", trail.Execution(executionID))
	trail.Log("order.fulfillment.started", trail.Execution(executionID), trail.Flow(flowID), trail.Entity("order", "order_123"))
	trail.Log("order.fulfillment.inventory_reserved", trail.Execution(executionID), trail.Flow(flowID), trail.String("warehouse", "warehouse-a"), trail.Int("available_units", 97))
}
