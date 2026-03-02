package util

import (
	"fmt"

	"github.com/kubeflow/mcp-apache-spark-history-server/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/config"
)

func NewSHSClient(configPath, serverName string) (*client.ClientWithResponses, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("loading config: %w", err)
	}

	var srv config.Server
	var found bool
	if serverName != "" {
		srv, found = cfg.Servers[serverName]
		if !found {
			return nil, fmt.Errorf("server %q not found in config", serverName)
		}
	} else {
		for _, s := range cfg.Servers {
			if s.Default {
				srv = s
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("no default server in config")
		}
	}

	return client.NewClientWithResponses(srv.URL + "/api/v1")
}
