package config

type GeneralConfig struct {
	NodeNum       int    `yaml:"nodeNum"`
	HasOutPutFile bool   `yaml:"hasOutPutFile"`
	OutPutPath    string `yaml:"outPutPath"`
	IsSQL         int    `yaml:"isSQL"`
	TotalNode     int    `yaml:"totalNode"`
}
