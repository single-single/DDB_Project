package config

type LogConfig struct {
	Level      string `yaml:"level"`
	OutputPath string `yaml:"outputPath"`
}

//level:        "debug"
//outputPath:   "../log/"
