package config

import (
	"github.com/sirupsen/logrus"
	"github.com/zsmartex/pkg/services"
)

var Logger *logrus.Entry
var Kafka *services.KafkaClient

func InitializeConfig() {
	Logger = services.NewLoggerService("RANGO")
	Kafka = services.NewKafka(Logger)
}
