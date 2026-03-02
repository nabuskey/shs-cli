package cmd

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/util"
	"github.com/spf13/cobra"
)

var cliVersion = "dev"

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show CLI and server versions",
		RunE: func(cmd *cobra.Command, args []string) error {
			return showVersion(cmd)
		},
	}
}

type versionInfo struct {
	CLI    string `json:"cli" yaml:"cli"`
	Server string `json:"server,omitempty" yaml:"server,omitempty"`
}

func showVersion(cmd *cobra.Command) error {
	info := versionInfo{CLI: cliVersion}

	c, err := newClient(util.WithTimeout(5 * time.Second))
	if err == nil {
		resp, err := c.GetVersionWithResponse(context.Background())
		if err == nil && resp.JSON200 != nil && resp.JSON200.Spark != nil {
			info.Server = *resp.JSON200.Spark
		}
	}

	return printOutput(cmd.OutOrStdout(), info, func(w io.Writer) error {
		fmt.Fprintf(w, "CLI:    %s\n", info.CLI)
		if info.Server != "" {
			fmt.Fprintf(w, "Server: %s\n", info.Server)
		} else {
			fmt.Fprintln(w, "Server: unavailable")
		}
		return nil
	})
}
