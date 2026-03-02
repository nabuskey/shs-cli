package cmd

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"text/tabwriter"

	"github.com/kubeflow/mcp-apache-spark-history-server/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/util"
	"github.com/spf13/cobra"
)

func newAppsCmd() *cobra.Command {
	var status string
	var limit int
	var sortBy string
	var desc bool

	cmd := &cobra.Command{
		Use:   "apps",
		Short: "List applications",
		RunE: func(cmd *cobra.Command, args []string) error {
			params := &client.ListApplicationsParams{}
			if status != "" {
				s := client.ListApplicationsParamsStatus(status)
				params.Status = &s
			}
			if limit != 0 {
				params.Limit = &limit
			}
			c, err := newClient()
			if err != nil {
				return err
			}
			return listApps(cmd, c, params, limit, sortBy, desc)
		},
	}

	cmd.Flags().StringVar(&status, "status", "", "Filter by status (running|completed)")
	cmd.Flags().IntVar(&limit, "limit", 20, "Max number of applications to return (0 for all)")
	cmd.Flags().StringVar(&sortBy, "sort", "", "Sort by field (name|id|date|duration)")
	cmd.Flags().BoolVar(&desc, "desc", false, "Sort descending")
	return cmd
}

func sortApps(apps []client.Application, field string, desc bool) {
	slices.SortFunc(apps, func(a, b client.Application) int {
		var c int
		switch field {
		case "name":
			c = cmp.Compare(util.Deref(a.Name), util.Deref(b.Name))
		case "id":
			c = cmp.Compare(util.Deref(a.Id), util.Deref(b.Id))
		case "date":
			c = cmp.Compare(latestAttemptEpoch(a), latestAttemptEpoch(b))
		case "duration":
			c = cmp.Compare(latestAttemptDuration(a), latestAttemptDuration(b))
		}
		if desc {
			return -c
		}
		return c
	})
}

func latestAttemptEpoch(a client.Application) int64 {
	if a.Attempts == nil {
		return 0
	}
	for _, att := range *a.Attempts {
		if att.StartTimeEpoch != nil {
			return *att.StartTimeEpoch
		}
	}
	return 0
}

func latestAttemptDuration(a client.Application) int64 {
	if a.Attempts == nil {
		return 0
	}
	for _, att := range *a.Attempts {
		if att.Duration != nil {
			return *att.Duration
		}
	}
	return 0
}

func listApps(cmd *cobra.Command, c client.ClientWithResponsesInterface, params *client.ListApplicationsParams, limit int, sortBy string, desc bool) error {
	resp, err := c.ListApplicationsWithResponse(context.Background(), params)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("unexpected status: %s", resp.HTTPResponse.Status)
	}

	apps := *resp.JSON200

	if sortBy != "" {
		sortApps(apps, sortBy, desc)
	}

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
		if err := tw.Flush(); err != nil {
			return err
		}
		if limit > 0 && len(apps) >= limit {
			fmt.Fprintf(w, "\nShowing %d applications. Use --limit 0 to list all.\n", limit)
		}
		return nil
	})
}
