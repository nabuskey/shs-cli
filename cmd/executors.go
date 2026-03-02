package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newExecutorsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "executors",
		Short: "List executors for an application",
		RunE: func(cmd *cobra.Command, args []string) error {
			all, _ := cmd.Flags().GetBool("all")
			if all {
				fmt.Printf("TODO: GET /applications/%s/allexecutors\n", appID)
			} else {
				fmt.Printf("TODO: GET /applications/%s/executors\n", appID)
			}
			return nil
		},
	}
	cmd.Flags().Bool("all", false, "Include dead executors")
	return cmd
}
