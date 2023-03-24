package config

type DbConfig struct {
	Host        string `yaml:"host"`
	Port        int    `yaml:"port"`
	DbName      string `yaml:"dbName"`
	DbUser      string `yaml:"dbUser"`
	DbPassword  string `yaml:"dbPassword"`
	SslMode     string `yaml:"sslMode"`
	SslRootCert string `yaml:"sslRootCert"`
}

//host        = "127.0.0.1"
//port        = 5433
//dbName      = "yugabyte"
//dbUser      = "yugabyte"
//dbPassword  = "yugabyte"
//sslMode     = "disable"
//sslRootCert = ""
