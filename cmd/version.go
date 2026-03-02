package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Get Spark version",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("TODO: GET /version")
			return nil
		},
	}
}
