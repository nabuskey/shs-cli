package cmd

import (
	"github.com/spf13/cobra"
)

var (
	appID      string
	serverName string
	configPath string
	outputFmt  string
)

var rootCmd = &cobra.Command{
	Use:   "shs",
	Short: "CLI for Apache Spark History Server",
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&appID, "app-id", "a", "", "Spark application ID (or SHS_APP_ID env var)")
	rootCmd.PersistentFlags().StringVarP(&serverName, "server", "s", "", "Server name from config")
	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "config.yaml", "Path to config file")
	rootCmd.PersistentFlags().StringVarP(&outputFmt, "output", "o", "txt", "Output format (txt|json|yaml)")

	rootCmd.AddCommand(
		newVersionCmd(),
		newAppsCmd(),
		newJobsCmd(),
		newStagesCmd(),
		newExecutorsCmd(),
		newSQLCmd(),
		newEnvironmentCmd(),
		newStorageCmd(),
		newLogsCmd(),
	)
}

func Execute() error {
	return rootCmd.Execute()
}
