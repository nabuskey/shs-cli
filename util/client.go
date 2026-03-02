package util

import (
	"fmt"
	"net/http"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/config"
)

const DefaultTimeout = 10 * time.Second

func NewSHSClient(configPath, serverName string, opts ...Option) (client.ClientWithResponsesInterface, error) {
	o := options{timeout: DefaultTimeout}
	for _, fn := range opts {
		fn(&o)
	}

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

	httpClient := &http.Client{Timeout: o.timeout}
	return client.NewClientWithResponses(srv.URL+"/api/v1", client.WithHTTPClient(httpClient))
}

type options struct {
	timeout time.Duration
}

type Option func(*options)

func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}
