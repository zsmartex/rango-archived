package config

import (
	"github.com/sirupsen/logrus"
	"github.com/zsmartex/pkg/services"
)

var Logger *logrus.Entry

func InitializeConfig() {
	Logger = services.NewLoggerService("RANGO")
}
