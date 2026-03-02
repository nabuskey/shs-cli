package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newEnvironmentCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "environment",
		Short:   "Get environment info for an application",
		Aliases: []string{"env"},
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("TODO: GET /applications/%s/environment\n", appID)
			return nil
		},
	}
}
