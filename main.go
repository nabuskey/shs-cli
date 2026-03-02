package main

import (
	"os"

	"github.com/kubeflow/mcp-apache-spark-history-server/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
