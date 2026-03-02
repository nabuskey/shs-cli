package cmd

import (
	"fmt"
	"io"

	gojson "github.com/goccy/go-json"
	goyaml "github.com/goccy/go-yaml"
)

func printOutput(w io.Writer, v any, textFn func(io.Writer) error) error {
	switch outputFmt {
	case "json":
		b, err := gojson.MarshalIndent(v, "", "  ")
		if err != nil {
			return err
		}
		b = append(b, '\n')
		_, err = w.Write(b)
		return err
	case "yaml":
		b, err := goyaml.Marshal(v)
		if err != nil {
			return err
		}
		_, err = w.Write(b)
		return err
	case "txt":
		return textFn(w)
	default:
		return fmt.Errorf("unsupported output format: %s", outputFmt)
	}
}
