package cmd

import (
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/util"
)

type stageAggregation struct {
	Count        int
	Tasks        int
	Duration     time.Duration
	InputBytes   int64
	ShuffleRead  int64
	ShuffleWrite int64
	SpillDisk    int64
	GCTime       int64
}

func aggStages(stages []client.StageData, jobStageIDs map[int]bool) stageAggregation {
	var a stageAggregation
	seen := map[int]bool{}
	for _, s := range stages {
		sid := util.Deref(s.StageId)
		if !jobStageIDs[sid] || seen[sid] {
			continue
		}
		seen[sid] = true
		a.Count++
		a.Tasks += util.Deref(s.NumTasks)
		a.Duration += stageDuration(s)
		a.InputBytes += util.Deref(s.InputBytes)
		a.ShuffleRead += util.Deref(s.ShuffleReadBytes)
		a.ShuffleWrite += util.Deref(s.ShuffleWriteBytes)
		a.SpillDisk += util.Deref(s.DiskBytesSpilled)
		a.GCTime += util.Deref(s.JvmGcTime)
	}
	return a
}

func stageIDsFromJobs(jobs []client.Job) map[int]bool {
	m := map[int]bool{}
	for _, j := range jobs {
		if j.StageIds != nil {
			for _, id := range *j.StageIds {
				m[id] = true
			}
		}
	}
	return m
}
