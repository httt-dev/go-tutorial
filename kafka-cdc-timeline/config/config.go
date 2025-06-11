package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	Kafka struct {
		ClientID       string   `mapstructure:"clientId"`
		Brokers        []string `mapstructure:"brokers"`
		ConsumerGroups struct {
			Oracle   string `mapstructure:"oracle"`
			Postgres string `mapstructure:"postgres"`
		} `mapstructure:"consumerGroups"`
	} `mapstructure:"kafka"`

	KafkaConnect struct {
		ConnectorHost string `mapstructure:"connectorHost"`
	} `mapstructure:"kafkaConnect"`

	Server struct {
		Port          int `mapstructure:"port"`
		WebsocketPort int `mapstructure:"websocketPort"`
	} `mapstructure:"server"`

	Topics struct {
		Oracle   string `mapstructure:"oracle"`
		Postgres string `mapstructure:"postgres"`
	} `mapstructure:"topics"`
}

var AppConfig Config

func LoadConfig() error {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	return viper.Unmarshal(&AppConfig)
} 