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

func newJobsCmd() *cobra.Command {
	var status string
	var limit int
	var group string
	var sortBy string

	cmd := &cobra.Command{
		Use:   "jobs [jobId]",
		Short: "List or get jobs for an application",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			if len(args) == 1 {
				jobId, err := strconv.Atoi(args[0])
				if err != nil {
					return fmt.Errorf("invalid job ID: %s", args[0])
				}
				return getJob(cmd, c, jobId)
			}
			params := &client.ListJobsParams{}
			if status != "" {
				s := client.ListJobsParamsStatus(status)
				params.Status = &s
			}
			return listJobs(cmd, c, params, limit, group, sortBy)
		},
	}

	cmd.Flags().StringVar(&status, "status", "", "Filter by status (running|succeeded|failed|unknown)")
	cmd.Flags().IntVar(&limit, "limit", 20, "Max number of jobs to return (0 for all)")
	cmd.Flags().StringVar(&group, "group", "", "Filter by job group")
	cmd.Flags().StringVar(&sortBy, "sort", "", "Sort by field (failed-tasks|duration|id)")
	return cmd
}

var statusPriority = map[string]int{
	"FAILED":    0,
	"RUNNING":   1,
	"UNKNOWN":   2,
	"SUCCEEDED": 3,
}

func jobDuration(j client.Job) time.Duration {
	if j.SubmissionTime == nil || j.CompletionTime == nil {
		return 0
	}
	start, err1 := util.ParseSparkTime(*j.SubmissionTime)
	end, err2 := util.ParseSparkTime(*j.CompletionTime)
	if err1 != nil || err2 != nil {
		return 0
	}
	return end.Sub(start)
}

func sortJobs(jobs []client.Job, sortBy string) {
	if sortBy != "" {
		slices.SortFunc(jobs, func(a, b client.Job) int {
			switch sortBy {
			case "failed-tasks":
				return -cmp.Compare(util.Deref(a.NumFailedTasks), util.Deref(b.NumFailedTasks))
			case "duration":
				return -cmp.Compare(jobDuration(a), jobDuration(b))
			case "id":
				return -cmp.Compare(util.Deref(a.JobId), util.Deref(b.JobId))
			}
			return 0
		})
		return
	}
	// default: failed status first, then by duration desc
	slices.SortFunc(jobs, func(a, b client.Job) int {
		pa := statusPriority[string(util.Deref(a.Status))]
		pb := statusPriority[string(util.Deref(b.Status))]
		if c := cmp.Compare(pa, pb); c != 0 {
			return c
		}
		return -cmp.Compare(jobDuration(a), jobDuration(b))
	})
}

func listJobs(cmd *cobra.Command, c client.ClientWithResponsesInterface, params *client.ListJobsParams, limit int, group string, sortBy string) error {
	resp, err := c.ListJobsWithResponse(context.Background(), appID, params)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("unexpected status: %s", resp.HTTPResponse.Status)
	}

	jobs := *resp.JSON200

	if group != "" {
		filtered := jobs[:0]
		for _, j := range jobs {
			if j.JobGroup != nil && *j.JobGroup == group {
				filtered = append(filtered, j)
			}
		}
		jobs = filtered
	}

	sortJobs(jobs, sortBy)

	if limit > 0 && len(jobs) > limit {
		jobs = jobs[:limit]
	}

	return printOutput(cmd.OutOrStdout(), jobs, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tSTATUS\tDESCRIPTION\tDURATION\tTASKS\tFAILED_TASKS\tSTAGES")
		for _, j := range jobs {
			desc := util.Deref(j.Description)
			if desc == "" {
				desc = util.Deref(j.Name)
			}
			stages := ""
			if j.StageIds != nil {
				ids := make([]string, len(*j.StageIds))
				for i, id := range *j.StageIds {
					ids[i] = strconv.Itoa(id)
				}
				stages = strings.Join(ids, ",")
			}
			fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%d\t%d\t%s\n",
				util.Deref(j.JobId),
				util.Deref(j.Status),
				desc,
				jobDuration(j).Truncate(time.Millisecond),
				util.Deref(j.NumTasks),
				util.Deref(j.NumFailedTasks),
				stages,
			)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		if limit > 0 && len(*resp.JSON200) > limit {
			fmt.Fprintf(w, "\nShowing %d of %d jobs. Use --limit 0 to list all.\n", limit, len(*resp.JSON200))
		}
		return nil
	})
}

func getJob(cmd *cobra.Command, c client.ClientWithResponsesInterface, jobId int) error {
	resp, err := c.GetJobWithResponse(context.Background(), appID, jobId)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("unexpected status: %s", resp.HTTPResponse.Status)
	}

	j := resp.JSON200
	return printOutput(cmd.OutOrStdout(), j, func(w io.Writer) error {
		desc := util.Deref(j.Description)
		if desc == "" {
			desc = util.Deref(j.Name)
		}
		stages := ""
		if j.StageIds != nil {
			ids := make([]string, len(*j.StageIds))
			for i, id := range *j.StageIds {
				ids[i] = strconv.Itoa(id)
			}
			stages = strings.Join(ids, ",")
		}
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintf(tw, "Job ID:\t%d\n", util.Deref(j.JobId))
		fmt.Fprintf(tw, "Status:\t%s\n", util.Deref(j.Status))
		fmt.Fprintf(tw, "Description:\t%s\n", desc)
		fmt.Fprintf(tw, "Duration:\t%s\n", jobDuration(*j).Truncate(time.Millisecond))
		fmt.Fprintf(tw, "Tasks:\t%d (failed: %d, killed: %d, skipped: %d)\n",
			util.Deref(j.NumTasks), util.Deref(j.NumFailedTasks), util.Deref(j.NumKilledTasks), util.Deref(j.NumSkippedTasks))
		fmt.Fprintf(tw, "Stages:\t%s (failed: %d, skipped: %d)\n",
			stages, util.Deref(j.NumFailedStages), util.Deref(j.NumSkippedStages))
		if j.JobGroup != nil {
			fmt.Fprintf(tw, "Group:\t%s\n", *j.JobGroup)
		}
		return tw.Flush()
	})
}
