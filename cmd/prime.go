package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

const primeText = `shs — Spark History Server CLI (for AI agents)

COMMANDS
  shs apps                     List applications
  shs jobs -a APP_ID           List jobs for an application
  shs jobs -a APP_ID JOB_ID    Get job detail
  shs stages -a APP_ID         List stages for an application
  shs stages -a APP_ID STAGE   Get stage detail with full metrics
  shs env -a APP_ID            Show environment/config
  shs executors -a APP_ID      List executors
  shs sql -a APP_ID            List SQL executions
  shs version                  CLI + server Spark version

GLOBAL FLAGS
  -a, --app-id STRING     Application ID (required for most commands)
  -s, --server STRING     Server name from config file
  -o, --output FORMAT     txt (default) | json | yaml
  -c, --config PATH       Config file (default: config.yaml)
      --timeout DURATION  HTTP timeout (default: 10s)

LIST FLAGS (apps, jobs, stages)
  --limit N       Max results, default 20. Use --limit 0 for all.
  --status VALUE  Filter by status (values vary per command).
  --sort FIELD    Sort field (values vary per command).

COMMAND DETAILS
  apps   --status running|completed  --sort name|id|date|duration  --desc
  jobs   --status running|succeeded|failed|unknown  --sort failed-tasks|duration|id  --group GROUP
  stages --status active|complete|pending|failed  --sort failed-tasks|duration|id

DEFAULT SORT
  jobs:   failed first, then by duration descending
  stages: failed → complete → active → pending → skipped, then duration desc
  Skipped stages are hidden by default.

OUTPUT
  -o txt    Human-readable tables (default).
  -o json   Full API response objects. Best for programmatic use.
  -o yaml   Same data as json, YAML formatted.

COMMON WORKFLOWS

  Find the application ID:
    shs apps
    shs apps --status completed --limit 5

  Investigate failures:
    shs jobs -a APP_ID --status failed
    shs stages -a APP_ID --status failed
    shs stages -a APP_ID STAGE_ID

  Find slowest stages:
    shs stages -a APP_ID --sort duration --limit 10

  Check for data skew (look at shuffle and spill metrics):
    shs stages -a APP_ID STAGE_ID -o json

  Get Spark config for an app:
    shs env -a APP_ID --section spark

DATA MODEL
  Application  A Spark app with one or more attempts.
  Job          A Spark action (collect, save, etc). Contains stages.
  Stage        A unit of parallel work. Has tasks and may have retry attempts.
               Detail view shows: task counts, input/output bytes,
               shuffle read/write, spill, GC time, scheduling pool.
  Job Group    Optional grouping set by the application. Filter with --group.

TIPS
  - Stage IDs appear in job output for cross-referencing.
  - Stage detail shows the latest attempt by default.
  - Duration is computed from submissionTime/completionTime.
  - Use -o json when you need to extract specific fields.
  - All timestamps are UTC.
`

func newPrimeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "prime",
		Short: "Print CLI usage reference for AI agents",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprint(cmd.OutOrStdout(), primeText)
			return nil
		},
	}
}
