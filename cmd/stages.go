package cmd

import (
	"cmp"
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

func newStagesCmd() *cobra.Command {
	var status string
	var limit int
	var sortBy string
	var errors bool

	cmd := &cobra.Command{
		Use:   "stages [stageId]",
		Short: "List or get stages for an application",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			if len(args) == 1 {
				stageId, err := strconv.Atoi(args[0])
				if err != nil {
					return fmt.Errorf("invalid stage ID: %s", args[0])
				}
				if errors {
					return getStageErrors(cmd, c, stageId)
				}
				return getStage(cmd, c, stageId)
			}
			params := &client.ListStagesParams{}
			if status != "" {
				s := client.ListStagesParamsStatus(status)
				params.Status = &s
			}
			return listStages(cmd, c, params, limit, sortBy)
		},
	}

	cmd.Flags().StringVar(&status, "status", "", "Filter by status (active|complete|pending|failed)")
	cmd.Flags().IntVar(&limit, "limit", 20, "Max number of stages to return (0 for all)")
	cmd.Flags().StringVar(&sortBy, "sort", "", "Sort by field (failed-tasks|duration|id)")
	cmd.Flags().BoolVar(&errors, "errors", false, "Show only failed tasks with error messages (requires stageId)")
	return cmd
}

var stageStatusPriority = map[string]int{
	"FAILED":   0,
	"COMPLETE": 1,
	"ACTIVE":   2,
	"PENDING":  3,
	"SKIPPED":  4,
}

func stageDuration(s client.StageData) time.Duration {
	if s.SubmissionTime == nil || s.CompletionTime == nil {
		return 0
	}
	start, err1 := util.ParseSparkTime(*s.SubmissionTime)
	end, err2 := util.ParseSparkTime(*s.CompletionTime)
	if err1 != nil || err2 != nil {
		return 0
	}
	return end.Sub(start)
}

func sortStages(stages []client.StageData, sortBy string) {
	if sortBy != "" {
		slices.SortFunc(stages, func(a, b client.StageData) int {
			switch sortBy {
			case "failed-tasks":
				return -cmp.Compare(util.Deref(a.NumFailedTasks), util.Deref(b.NumFailedTasks))
			case "duration":
				return -cmp.Compare(stageDuration(a), stageDuration(b))
			case "id":
				return -cmp.Compare(util.Deref(a.StageId), util.Deref(b.StageId))
			}
			return 0
		})
		return
	}
	// default: failed status first, then by duration desc
	slices.SortFunc(stages, func(a, b client.StageData) int {
		pa := stageStatusPriority[string(util.Deref(a.Status))]
		pb := stageStatusPriority[string(util.Deref(b.Status))]
		if c := cmp.Compare(pa, pb); c != 0 {
			return c
		}
		return -cmp.Compare(stageDuration(a), stageDuration(b))
	})
}

func stageDesc(s client.StageData) string {
	if d := util.Deref(s.Description); d != "" {
		return d
	}
	return util.Deref(s.Name)
}

func listStages(cmd *cobra.Command, c client.ClientWithResponsesInterface, params *client.ListStagesParams, limit int, sortBy string) error {
	resp, err := c.ListStagesWithResponse(cmd.Context(), appID, params)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("unexpected status: %s", resp.HTTPResponse.Status)
	}

	stages := *resp.JSON200
	sortStages(stages, sortBy)

	total := len(stages)
	if limit > 0 && len(stages) > limit {
		stages = stages[:limit]
	}

	return printOutput(cmd.OutOrStdout(), stages, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tATTEMPT\tSTATUS\tDESCRIPTION\tTASKS\tFAILED\tDURATION\tINPUT\tSHUFFLE_READ\tSHUFFLE_WRITE")
		for _, s := range stages {
			fmt.Fprintf(tw, "%d\t%d\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
				util.Deref(s.StageId),
				util.Deref(s.AttemptId),
				util.Deref(s.Status),
				stageDesc(s),
				util.Deref(s.NumTasks),
				util.Deref(s.NumFailedTasks),
				stageDuration(s).Truncate(time.Millisecond),
				util.DerefBytes(s.InputBytes),
				util.DerefBytes(s.ShuffleReadBytes),
				util.DerefBytes(s.ShuffleWriteBytes),
			)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		if limit > 0 && total > limit {
			fmt.Fprintf(w, "\nShowing %d of %d stages. Use --limit 0 to list all.\n", limit, total)
		}
		return nil
	})
}

func getStage(cmd *cobra.Command, c client.ClientWithResponsesInterface, stageId int) error {
	params := &client.ListStageAttemptsParams{}
	resp, err := c.ListStageAttemptsWithResponse(cmd.Context(), appID, stageId, params)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("unexpected status: %s", resp.HTTPResponse.Status)
	}

	attempts := *resp.JSON200
	if len(attempts) == 0 {
		return fmt.Errorf("no attempts found for stage %d", stageId)
	}
	// show latest attempt
	s := attempts[len(attempts)-1]

	return printOutput(cmd.OutOrStdout(), s, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintf(tw, "Stage ID:\t%d\n", util.Deref(s.StageId))
		fmt.Fprintf(tw, "Attempt:\t%d\n", util.Deref(s.AttemptId))
		fmt.Fprintf(tw, "Status:\t%s\n", util.Deref(s.Status))
		fmt.Fprintf(tw, "Description:\t%s\n", stageDesc(s))
		fmt.Fprintf(tw, "Name:\t%s\n", util.Deref(s.Name))
		fmt.Fprintf(tw, "Duration:\t%s\n", stageDuration(s).Truncate(time.Millisecond))
		fmt.Fprintf(tw, "Tasks:\t%d (failed: %d, killed: %d)\n",
			util.Deref(s.NumTasks), util.Deref(s.NumFailedTasks), util.Deref(s.NumKilledTasks))
		fmt.Fprintf(tw, "Input:\t%s (%d records)\n",
			util.DerefBytes(s.InputBytes), util.Deref(s.InputRecords))
		fmt.Fprintf(tw, "Output:\t%s (%d records)\n",
			util.DerefBytes(s.OutputBytes), util.Deref(s.OutputRecords))
		fmt.Fprintf(tw, "Shuffle Read:\t%s (%d records)\n",
			util.DerefBytes(s.ShuffleReadBytes), util.Deref(s.ShuffleReadRecords))
		fmt.Fprintf(tw, "Shuffle Write:\t%s (%d records)\n",
			util.DerefBytes(s.ShuffleWriteBytes), util.Deref(s.ShuffleWriteRecords))
		fmt.Fprintf(tw, "Memory Spilled:\t%s\n", util.DerefBytes(s.MemoryBytesSpilled))
		fmt.Fprintf(tw, "Disk Spilled:\t%s\n", util.DerefBytes(s.DiskBytesSpilled))
		fmt.Fprintf(tw, "GC Time:\t%dms\n", util.Deref(s.JvmGcTime))
		fmt.Fprintf(tw, "Pool:\t%s\n", util.Deref(s.SchedulingPool))
		return tw.Flush()
	})
}

func getStageErrors(cmd *cobra.Command, c client.ClientWithResponsesInterface, stageId int) error {
	// get latest attempt ID
	params := &client.ListStageAttemptsParams{}
	resp, err := c.ListStageAttemptsWithResponse(cmd.Context(), appID, stageId, params)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("unexpected status: %s", resp.HTTPResponse.Status)
	}
	attempts := *resp.JSON200
	if len(attempts) == 0 {
		return fmt.Errorf("no attempts found for stage %d", stageId)
	}
	attemptId := util.Deref(attempts[len(attempts)-1].AttemptId)

	// fetch failed tasks
	status := client.ListTasksParamsStatus("failed")
	taskParams := &client.ListTasksParams{Status: &status}
	taskResp, err := c.ListTasksWithResponse(cmd.Context(), appID, stageId, attemptId, taskParams)
	if err != nil {
		return err
	}
	if taskResp.JSON200 == nil {
		return fmt.Errorf("unexpected status: %s", taskResp.HTTPResponse.Status)
	}

	tasks := *taskResp.JSON200
	return printOutput(cmd.OutOrStdout(), tasks, func(w io.Writer) error {
		if len(tasks) == 0 {
			fmt.Fprintln(w, "No failed tasks.")
			return nil
		}
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "TASK\tATTEMPT\tEXECUTOR\tSTATUS\tERROR")
		for _, t := range tasks {
			errMsg := util.Deref(t.ErrorMessage)
			if i := strings.IndexByte(errMsg, '\n'); i >= 0 {
				errMsg = errMsg[:i]
			}
			fmt.Fprintf(tw, "%d\t%d\t%s\t%s\t%s\n",
				util.Deref(t.TaskId),
				util.Deref(t.Attempt),
				util.Deref(t.ExecutorId),
				util.Deref(t.Status),
				errMsg,
			)
		}
		return tw.Flush()
	})
}
