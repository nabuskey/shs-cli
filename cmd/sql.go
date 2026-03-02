package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newSQLCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "sql [executionId]",
		Short: "List or get SQL executions for an application",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				fmt.Printf("TODO: GET /applications/%s/sql/%s\n", appID, args[0])
			} else {
				fmt.Printf("TODO: GET /applications/%s/sql\n", appID)
			}
			return nil
		},
	}
}
