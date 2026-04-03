package cmd

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/util"
	"github.com/spf13/cobra"
)

func newSQLCmd() *cobra.Command {
	var status string
	var limit int
	var sortBy string

	cmd := &cobra.Command{
		Use:   "sql [executionId]",
		Short: "List or get SQL executions for an application",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			if len(args) == 1 {
				id, err := strconv.Atoi(args[0])
				if err != nil {
					return fmt.Errorf("invalid execution ID: %s", args[0])
				}
				return getSQLExecution(cmd, c, id)
			}
			return listSQLExecutions(cmd, c, status, limit, sortBy)
		},
	}

	cmd.Flags().StringVar(&status, "status", "", "Filter by status (completed|running|failed)")
	cmd.Flags().IntVar(&limit, "limit", 20, "Max number of executions to return (0 for all)")
	cmd.Flags().StringVar(&sortBy, "sort", "", "Sort by field (duration|id)")
	return cmd
}

var sqlStatusPriority = map[string]int{
	"FAILED":    0,
	"RUNNING":   1,
	"COMPLETED": 2,
}

func sqlDuration(s client.SQLExecution) time.Duration {
	return time.Duration(util.Deref(s.Duration)) * time.Millisecond
}

func sortSQLExecutions(execs []client.SQLExecution, sortBy string) {
	if sortBy != "" {
		slices.SortFunc(execs, func(a, b client.SQLExecution) int {
			switch sortBy {
			case "duration":
				return -cmp.Compare(sqlDuration(a), sqlDuration(b))
			case "id":
				return -cmp.Compare(util.Deref(a.Id), util.Deref(b.Id))
			}
			return 0
		})
		return
	}
	// default: failed first, then by duration desc
	slices.SortFunc(execs, func(a, b client.SQLExecution) int {
		pa := sqlStatusPriority[string(util.Deref(a.Status))]
		pb := sqlStatusPriority[string(util.Deref(b.Status))]
		if c := cmp.Compare(pa, pb); c != 0 {
			return c
		}
		return -cmp.Compare(sqlDuration(a), sqlDuration(b))
	})
}

func formatJobIds(ids *[]int) string {
	if ids == nil || len(*ids) == 0 {
		return ""
	}
	s := make([]string, len(*ids))
	for i, id := range *ids {
		s[i] = strconv.Itoa(id)
	}
	return strings.Join(s, ",")
}

func listSQLExecutions(cmd *cobra.Command, c client.ClientWithResponsesInterface, status string, limit int, sortBy string) error {
	details := false
	params := &client.ListSQLExecutionsParams{Details: &details}
	resp, err := c.ListSQLExecutionsWithResponse(context.Background(), appID, params)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("unexpected status: %s", resp.HTTPResponse.Status)
	}

	execs := *resp.JSON200
	if status != "" {
		upper := strings.ToUpper(status)
		execs = slices.DeleteFunc(execs, func(e client.SQLExecution) bool {
			return string(util.Deref(e.Status)) != upper
		})
	}

	sortSQLExecutions(execs, sortBy)

	total := len(execs)
	if limit > 0 && len(execs) > limit {
		execs = execs[:limit]
	}

	return printOutput(cmd.OutOrStdout(), execs, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tSTATUS\tDESCRIPTION\tDURATION\tFAILED_JOBS\tSUCCESS_JOBS\tRUNNING_JOBS")
		for _, e := range execs {
			desc := util.Deref(e.Description)
			if len(desc) > 80 {
				desc = desc[:77] + "..."
			}
			fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
				util.Deref(e.Id),
				util.Deref(e.Status),
				desc,
				sqlDuration(e).Truncate(time.Millisecond),
				formatJobIds(e.FailedJobIds),
				formatJobIds(e.SuccessJobIds),
				formatJobIds(e.RunningJobIds),
			)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		if limit > 0 && total > limit {
			fmt.Fprintf(w, "\nShowing %d of %d executions. Use --limit 0 to list all.\n", limit, total)
		}
		return nil
	})
}

func getSQLExecution(cmd *cobra.Command, c client.ClientWithResponsesInterface, id int) error {
	params := &client.GetSQLExecutionParams{}
	resp, err := c.GetSQLExecutionWithResponse(context.Background(), appID, id, params)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("unexpected status: %s", resp.HTTPResponse.Status)
	}

	e := resp.JSON200
	return printOutput(cmd.OutOrStdout(), e, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintf(tw, "Execution ID:\t%d\n", util.Deref(e.Id))
		fmt.Fprintf(tw, "Status:\t%s\n", util.Deref(e.Status))
		fmt.Fprintf(tw, "Description:\t%s\n", util.Deref(e.Description))
		fmt.Fprintf(tw, "Submitted:\t%s\n", util.Deref(e.SubmissionTime))
		fmt.Fprintf(tw, "Duration:\t%s\n", sqlDuration(*e).Truncate(time.Millisecond))
		fmt.Fprintf(tw, "Success Jobs:\t%s\n", formatJobIds(e.SuccessJobIds))
		fmt.Fprintf(tw, "Failed Jobs:\t%s\n", formatJobIds(e.FailedJobIds))
		fmt.Fprintf(tw, "Running Jobs:\t%s\n", formatJobIds(e.RunningJobIds))
		if e.PlanDescription != nil && *e.PlanDescription != "" {
			fmt.Fprintf(tw, "\nPlan:\n")
			tw.Flush()
			fmt.Fprintln(w, *e.PlanDescription)
		}
		return tw.Flush()
	})
}
