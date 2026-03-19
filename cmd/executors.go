package cmd

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"text/tabwriter"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/util"
	"github.com/spf13/cobra"
)

func newExecutorsCmd() *cobra.Command {
	var all bool
	var sortBy string

	cmd := &cobra.Command{
		Use:   "executors [executorId]",
		Short: "List or get executors for an application",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			if len(args) == 1 {
				return getExecutor(cmd, c, args[0])
			}
			return listExecutors(cmd, c, all, sortBy)
		},
	}

	cmd.Flags().BoolVar(&all, "all", false, "Include dead executors")
	cmd.Flags().StringVar(&sortBy, "sort", "", "Sort by field (failed-tasks|duration|gc|id)")
	return cmd
}

func sortExecutors(execs []client.Executor, sortBy string) {
	slices.SortFunc(execs, func(a, b client.Executor) int {
		switch sortBy {
		case "failed-tasks":
			return -cmp.Compare(util.Deref(a.FailedTasks), util.Deref(b.FailedTasks))
		case "duration":
			return -cmp.Compare(util.Deref(a.TotalDuration), util.Deref(b.TotalDuration))
		case "gc":
			return -cmp.Compare(util.Deref(a.TotalGCTime), util.Deref(b.TotalGCTime))
		case "id":
			return cmp.Compare(util.Deref(a.Id), util.Deref(b.Id))
		default:
			aa, ab := util.Deref(a.IsActive), util.Deref(b.IsActive)
			if aa != ab {
				if aa {
					return -1
				}
				return 1
			}
			return -cmp.Compare(util.Deref(a.TotalDuration), util.Deref(b.TotalDuration))
		}
	})
}

func fetchExecutors(c client.ClientWithResponsesInterface, all bool) ([]client.Executor, error) {
	if all {
		resp, err := c.ListAllExecutorsWithResponse(context.Background(), appID)
		if err != nil {
			return nil, err
		}
		if resp.JSON200 == nil {
			return nil, fmt.Errorf("unexpected status: %s", resp.HTTPResponse.Status)
		}
		return *resp.JSON200, nil
	}
	resp, err := c.ListActiveExecutorsWithResponse(context.Background(), appID)
	if err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected status: %s", resp.HTTPResponse.Status)
	}
	return *resp.JSON200, nil
}

func listExecutors(cmd *cobra.Command, c client.ClientWithResponsesInterface, all bool, sortBy string) error {
	execs, err := fetchExecutors(c, all)
	if err != nil {
		return err
	}

	sortExecutors(execs, sortBy)

	return printOutput(cmd.OutOrStdout(), execs, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tACTIVE\tHOST\tTASKS\tFAILED\tTASK_TIME\tGC_TIME\tINPUT\tSHUFFLE_READ\tSHUFFLE_WRITE")
		for _, e := range execs {
			fmt.Fprintf(tw, "%s\t%v\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\n",
				util.Deref(e.Id),
				util.Deref(e.IsActive),
				util.Deref(e.HostPort),
				util.Deref(e.TotalTasks),
				util.Deref(e.FailedTasks),
				(time.Duration(util.Deref(e.TotalDuration)) * time.Millisecond).Truncate(time.Millisecond),
				(time.Duration(util.Deref(e.TotalGCTime)) * time.Millisecond).Truncate(time.Millisecond),
				util.FormatBytes(util.Deref(e.TotalInputBytes)),
				util.FormatBytes(util.Deref(e.TotalShuffleRead)),
				util.FormatBytes(util.Deref(e.TotalShuffleWrite)),
			)
		}
		return tw.Flush()
	})
}

func getExecutor(cmd *cobra.Command, c client.ClientWithResponsesInterface, id string) error {
	// API has no single-executor endpoint; fetch all and filter
	execs, err := fetchExecutors(c, true)
	if err != nil {
		return err
	}

	idx := slices.IndexFunc(execs, func(e client.Executor) bool {
		return util.Deref(e.Id) == id
	})
	if idx == -1 {
		return fmt.Errorf("executor %s not found", id)
	}
	e := execs[idx]

	return printOutput(cmd.OutOrStdout(), e, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintf(tw, "Executor ID:\t%s\n", util.Deref(e.Id))
		fmt.Fprintf(tw, "Active:\t%v\n", util.Deref(e.IsActive))
		fmt.Fprintf(tw, "Host:\t%s\n", util.Deref(e.HostPort))
		fmt.Fprintf(tw, "Cores:\t%d\n", util.Deref(e.TotalCores))
		fmt.Fprintf(tw, "Max Memory:\t%s\n", util.FormatBytes(util.Deref(e.MaxMemory)))
		fmt.Fprintf(tw, "Memory Used:\t%s\n", util.FormatBytes(util.Deref(e.MemoryUsed)))
		fmt.Fprintf(tw, "Disk Used:\t%s\n", util.FormatBytes(util.Deref(e.DiskUsed)))
		fmt.Fprintf(tw, "Tasks:\t%d (active: %d, failed: %d, completed: %d)\n",
			util.Deref(e.TotalTasks), util.Deref(e.ActiveTasks), util.Deref(e.FailedTasks), util.Deref(e.CompletedTasks))
		fmt.Fprintf(tw, "Task Time:\t%s\n", (time.Duration(util.Deref(e.TotalDuration)) * time.Millisecond).Truncate(time.Millisecond))
		fmt.Fprintf(tw, "GC Time:\t%s\n", (time.Duration(util.Deref(e.TotalGCTime)) * time.Millisecond).Truncate(time.Millisecond))
		fmt.Fprintf(tw, "Input:\t%s\n", util.FormatBytes(util.Deref(e.TotalInputBytes)))
		fmt.Fprintf(tw, "Shuffle Read:\t%s\n", util.FormatBytes(util.Deref(e.TotalShuffleRead)))
		fmt.Fprintf(tw, "Shuffle Write:\t%s\n", util.FormatBytes(util.Deref(e.TotalShuffleWrite)))
		fmt.Fprintf(tw, "RDD Blocks:\t%d\n", util.Deref(e.RddBlocks))
		if e.RemoveReason != nil {
			fmt.Fprintf(tw, "Remove Reason:\t%s\n", *e.RemoveReason)
		}
		return tw.Flush()
	})
}
