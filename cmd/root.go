package cmd

import (
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/util"
	"github.com/spf13/cobra"
)

var (
	appID      string
	serverName string
	configPath string
	outputFmt  string
	timeout    time.Duration
)

var rootCmd = &cobra.Command{
	Use:          "shs",
	Short:        "CLI for Apache Spark History Server",
	SilenceUsage: true,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&appID, "app-id", "a", "", "Spark application ID (or SHS_APP_ID env var)")
	rootCmd.PersistentFlags().StringVarP(&serverName, "server", "s", "", "Server name from config")
	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "config.yaml", "Path to config file")
	rootCmd.PersistentFlags().StringVarP(&outputFmt, "output", "o", "txt", "Output format (txt|json|yaml)")
	rootCmd.PersistentFlags().DurationVar(&timeout, "timeout", 10*time.Second, "HTTP request timeout")

	rootCmd.AddCommand(
		newVersionCmd(),
		newAppsCmd(),
		newJobsCmd(),
		newStagesCmd(),
		newExecutorsCmd(),
		newSQLCmd(),
		newEnvironmentCmd(),
		//newStorageCmd(),
		//newLogsCmd(),
		newPrimeCmd(),
		newCompareCmd(),
		newServersCmd(),
	)
}

func Execute() error {
	return rootCmd.Execute()
}

func newClient(opts ...util.Option) (client.ClientWithResponsesInterface, error) {
	if len(opts) == 0 {
		opts = []util.Option{util.WithTimeout(timeout), util.WithServer(serverName)}
	}
	return util.NewSHSClient(configPath, opts...)
}
