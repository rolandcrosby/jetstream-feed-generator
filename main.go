package main

import (
	"os"

	"jetstream-feed-generator/application"
	"jetstream-feed-generator/config"
)

func main() {
	if err := config.Execute(run); err != nil {
		os.Exit(1)
	}
}

// run is the actual application entrypoint
func run(cfg config.Config) error {
	return application.Run(cfg)
}
