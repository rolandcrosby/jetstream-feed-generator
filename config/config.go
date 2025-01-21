package config

import (
	"fmt"
	"jetstream-feed-generator/consumer"
	"log/slog"
	"reflect"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Config struct {
	DBFilename string   `mapstructure:"db_filename"`
	FeedNames  []string `mapstructure:"feed_names"`
	LogLevel   string   `mapstructure:"log_level"`
	LogFormat  string   `mapstructure:"log_format"`
	Consumer   struct {
		Enabled      bool   `mapstructure:"enabled"`
		JetstreamURL string `mapstructure:"jetstream_url"`
		StartCursor  int64  `mapstructure:"start_cursor"`
	} `mapstructure:"consumer"`
	Feedgen struct {
		Enabled         bool   `mapstructure:"enabled"`
		Port            int    `mapstructure:"port"`
		FeedActorDID    string `mapstructure:"feed_actor_did"`
		ServiceEndpoint string `mapstructure:"service_endpoint"`
	} `mapstructure:"feedgen"`
}

func (config Config) Validate() error {
	if !(config.Consumer.Enabled || config.Feedgen.Enabled) {
		return fmt.Errorf("at least one of CONSUMER_ENABLED or FEEDGEN_ENABLED must be specified")
	}
	if config.DBFilename == "" {
		return fmt.Errorf("DB_FILENAME is required")
	}
	if len(config.FeedNames) == 0 {
		return fmt.Errorf("FEED_NAMES is required")
	}
	if config.Consumer.Enabled {
		if config.Consumer.JetstreamURL == "" {
			return fmt.Errorf("CONSUMER_JETSTREAM_URL is required")
		}
	}
	if config.Feedgen.Enabled {
		if config.Feedgen.Port == 0 {
			return fmt.Errorf("FEEDGEN_PORT is required")
		}
		if config.Feedgen.FeedActorDID == "" {
			return fmt.Errorf("FEEDGEN_FEED_ACTOR_DID is required")
		}
		if config.Feedgen.ServiceEndpoint == "" {
			return fmt.Errorf("FEEDGEN_SERVICE_ENDPOINT is required")
		}
	}
	return nil
}

func setupFlags(cmd *cobra.Command) {
	flags := cmd.PersistentFlags()

	flags.String("config", "", "YAML config file path")

	flags.String("db_filename", "feeds.sqlite", "Database filename")
	flags.StringArray("feed_names", []string{"composer-errors"}, "Feed names")
	flags.String("log_level", "INFO", "Log level")
	flags.String("log_format", "text", "Log format (text or json)")

	flags.Bool("consumer.enabled", true, "Enable consumer")
	flags.String("consumer.jetstream_url", consumer.DefaultJetstreamURL, "Jetstream URL")
	flags.Int64("consumer.start_cursor", 0, "Start cursor position")

	flags.Bool("feedgen.enabled", true, "Enable feed generator")
	flags.Int("feedgen.port", 9072, "Feed generator port")
	flags.String("feedgen.feed_actor_did", "", "Feed actor DID")
	flags.String("feedgen.service_endpoint", "", "Service endpoint URL")

	if err := viper.BindPFlags(flags); err != nil {
		panic(fmt.Sprintf("failed to bind flags: %v", err))
	}
}

func setupConfig(cmd *cobra.Command) {
	cobra.OnInitialize(func() {
		viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		viper.AutomaticEnv()
		viper.SetTypeByDefaultValue(true)

		// Load config file if specified
		if cfgFile := viper.GetString("config"); cfgFile != "" {
			viper.SetConfigFile(cfgFile)
			if err := viper.ReadInConfig(); err == nil {
				cmd.Printf("Using config file: %s\n", viper.ConfigFileUsed())
			}
		}
	})
}

func LogViperEnvVars(cfg interface{}, prefix string, logger *slog.Logger) {
	val := reflect.ValueOf(cfg)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	logStructFields(val, prefix, logger)
}

func logStructFields(v reflect.Value, prefix string, logger *slog.Logger) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		fullPath := field.Tag.Get("mapstructure")
		if fullPath == "" {
			fullPath = field.Name
		}
		fullPath = strings.ToUpper(fullPath)

		if prefix != "" {
			fullPath = prefix + "_" + fullPath
		}

		if field.Type.Kind() == reflect.Struct && field.Type.String() != "time.Time" {
			logStructFields(v.Field(i), fullPath, logger)
			continue
		}

		logger.Info("config value",
			fullPath, v.Field(i).Interface(),
		)
	}
}

func Execute(runFn func(Config) error) error {
	cmd := &cobra.Command{
		Use: "jetstream-feed-generator",
		RunE: func(cmd *cobra.Command, args []string) error {
			var cfg Config
			if err := viper.Unmarshal(&cfg); err != nil {
				return fmt.Errorf("unmarshal config: %w", err)
			}
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("invalid config: %w", err)
			}
			return runFn(cfg)
		},
	}

	setupFlags(cmd)
	setupConfig(cmd)

	return cmd.Execute()
}
