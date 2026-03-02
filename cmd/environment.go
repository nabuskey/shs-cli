package cmd

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/kubeflow/mcp-apache-spark-history-server/util"
	"github.com/spf13/cobra"
)

func newEnvironmentCmd() *cobra.Command {
	var section string

	cmd := &cobra.Command{
		Use:     "environment",
		Short:   "Get environment info for an application",
		Aliases: []string{"env"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return getEnvironment(cmd, section)
		},
	}

	cmd.Flags().StringVar(&section, "section", "", "Show specific section (runtime|spark|system|hadoop|metrics|classpath)")
	return cmd
}

func getEnvironment(cmd *cobra.Command, section string) error {
	c, err := util.NewSHSClient(configPath, serverName, util.WithTimeout(timeout))
	if err != nil {
		return err
	}

	resp, err := c.GetEnvironmentWithResponse(context.Background(), appID)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("unexpected status: %s", resp.HTTPResponse.Status)
	}

	env := resp.JSON200
	return printOutput(cmd.OutOrStdout(), env, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)

		if section == "" || section == "runtime" {
			if env.Runtime != nil {
				fmt.Fprintln(tw, "=== Runtime ===")
				fmt.Fprintf(tw, "Java Home\t%s\n", deref(env.Runtime.JavaHome))
				fmt.Fprintf(tw, "Java Version\t%s\n", deref(env.Runtime.JavaVersion))
				fmt.Fprintf(tw, "Scala Version\t%s\n", deref(env.Runtime.ScalaVersion))
				fmt.Fprintln(tw)
			}
		}

		sections := []struct {
			name string
			key  string
			data *[][]string
		}{
			{"Spark Properties", "spark", env.SparkProperties},
			{"System Properties", "system", env.SystemProperties},
			{"Hadoop Properties", "hadoop", env.HadoopProperties},
			{"Metrics Properties", "metrics", env.MetricsProperties},
			{"Classpath Entries", "classpath", env.ClasspathEntries},
		}

		for _, s := range sections {
			if section != "" && section != s.key {
				continue
			}
			if s.data != nil && len(*s.data) > 0 {
				fmt.Fprintf(tw, "=== %s ===\n", s.name)
				for _, pair := range *s.data {
					if len(pair) >= 2 {
						fmt.Fprintf(tw, "%s\t%s\n", pair[0], pair[1])
					}
				}
				fmt.Fprintln(tw)
			}
		}

		return tw.Flush()
	})
}
