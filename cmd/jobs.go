package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newJobsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "jobs [jobId]",
		Short: "List or get jobs for an application",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				fmt.Printf("TODO: GET /applications/%s/jobs/%s\n", appID, args[0])
			} else {
				fmt.Printf("TODO: GET /applications/%s/jobs\n", appID)
			}
			return nil
		},
	}
	cmd.Flags().String("status", "", "Filter by status (running|succeeded|failed|unknown)")
	return cmd
}
