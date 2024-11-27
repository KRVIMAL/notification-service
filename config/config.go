package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Kafka struct {
		URL   string `yaml:"url"`
		Topic string `yaml:"topic"`
		Group string `yaml:"group"`
	} `yaml:"kafka"`
	SMTP struct {
		Server   string `yaml:"server"`
		Port     string `yaml:"port"`
		Email    string `yaml:"email"`
		Password string `yaml:"password"`
	} `yaml:"smtp"`
	SMS struct {
		APIURL     string `yaml:"apiURL"`
		AuthScheme string `yaml:"authScheme"`
		UserID     string `yaml:"userID"`
		Password   string `yaml:"password"`
	} `yaml:"sms"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
