package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newStagesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stages [stageId]",
		Short: "List or get stages for an application",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				fmt.Printf("TODO: GET /applications/%s/stages/%s\n", appID, args[0])
			} else {
				fmt.Printf("TODO: GET /applications/%s/stages\n", appID)
			}
			return nil
		},
	}
	cmd.Flags().String("status", "", "Filter by status (active|complete|pending|failed)")
	cmd.Flags().Bool("details", false, "Include stage details")
	cmd.Flags().IntP("attempt", "t", -1, "Stage attempt ID")
	return cmd
}
