package config

import (
	"fmt"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

func Load(path string) (*Config, error) {
	k := koanf.New(".")

	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	// SHS_SERVERS_LOCAL_URL -> servers.local.url
	if err := k.Load(env.Provider("SHS_", ".", func(s string) string {
		return strings.ToLower(strings.ReplaceAll(
			strings.TrimPrefix(s, "SHS_"), "_", "."))
	}), nil); err != nil {
		return nil, fmt.Errorf("loading env: %w", err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}
	return &cfg, nil
}

type Config struct {
	Servers map[string]Server `koanf:"servers"`
}

type Server struct {
	Default       bool   `koanf:"default"`
	URL           string `koanf:"url"`
	VerifySSL     *bool  `koanf:"verify_ssl"`
	Auth          *Auth  `koanf:"auth"`
	EMRClusterARN string `koanf:"emr_cluster_arn"`
}

type Auth struct {
	Username string `koanf:"username"`
	Password string `koanf:"password"`
	Token    string `koanf:"token"`
}
