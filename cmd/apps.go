package cmd

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/kubeflow/mcp-apache-spark-history-server/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/util"
	"github.com/spf13/cobra"
)

func newAppsCmd() *cobra.Command {
	var status string
	var limit int

	cmd := &cobra.Command{
		Use:   "apps",
		Short: "List applications",
		RunE: func(cmd *cobra.Command, args []string) error {
			params := &client.ListApplicationsParams{}
			if status != "" {
				s := client.ListApplicationsParamsStatus(status)
				params.Status = &s
			}
			if limit > 0 {
				params.Limit = &limit
			}
			return listApps(cmd, params)
		},
	}

	cmd.Flags().StringVar(&status, "status", "", "Filter by status (running|completed)")
	cmd.Flags().IntVar(&limit, "limit", 0, "Max number of applications to return")
	return cmd
}

func listApps(cmd *cobra.Command, params *client.ListApplicationsParams) error {
	c, err := util.NewSHSClient(configPath, serverName)
	if err != nil {
		return err
	}

	resp, err := c.ListApplicationsWithResponse(context.Background(), params)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("unexpected status: %s", resp.HTTPResponse.Status)
	}

	apps := *resp.JSON200
	return printOutput(cmd.OutOrStdout(), apps, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tNAME\tATTEMPTS")
		for _, app := range apps {
			name, id := "", ""
			if app.Name != nil {
				name = *app.Name
			}
			if app.Id != nil {
				id = *app.Id
			}
			attempts := 0
			if app.Attempts != nil {
				attempts = len(*app.Attempts)
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\n", id, name, attempts)
		}
		return tw.Flush()
	})
}
