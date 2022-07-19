package main

import (
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sync"
	"syscall"
	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/kafka-client-go/v3"
	"github.com/Financial-Times/pac-annotations-mapper/health"
	"github.com/Financial-Times/pac-annotations-mapper/service"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	cli "github.com/jawher/mow.cli"
)

const (
	appName        = "PAC Annotations Mapper"
	appDescription = "UPP mapper for PAC annotations"
	appSystemCode  = "pac-annotations-mapper"
)

func main() {
	app := cli.App(appSystemCode, appDescription)

	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})
	logLevel := app.String(cli.StringOpt{
		Name:   "logLevel",
		Value:  "INFO",
		Desc:   "Log level",
		EnvVar: "LOG_LEVEL",
	})

	kafkaAddress := app.String(cli.StringOpt{
		Name:   "kafkaAddress",
		Value:  "kafka:9092",
		Desc:   "Addresses used by the kafka consumer and producer to connect to MSK",
		EnvVar: "KAFKA_ADDRESS",
	})
	// Kafka consumer config
	consumerGroup := app.String(cli.StringOpt{
		Name:   "consumerGroup",
		Value:  "pac-annotations-mapper",
		Desc:   "Group used to read the messages from the queue",
		EnvVar: "CONSUMER_GROUP",
	})
	consumerTopic := app.String(cli.StringOpt{
		Name:   "consumerTopic",
		Value:  "NativeCmsMetadataPublicationEvents",
		Desc:   "The topic to read the meassages from",
		EnvVar: "CONSUMER_TOPIC",
	})
	kafkaLagTolerance := app.Int(cli.IntOpt{
		Name:   "kafkaLagTolerance",
		Value:  200,
		Desc:   "Consumer offset lag tolerance.",
		EnvVar: "KAFKA_LAG_TOLERANCE",
	})
	// message filter
	whitelistRegex := app.String(cli.StringOpt{
		Name:   "whitelistRegex",
		Desc:   "The regex to use to filter messages based on Origin-System-Id.",
		EnvVar: "WHITELIST_REGEX",
		Value:  `http://cmdb\.ft\.com/systems/pac`,
	})
	producerTopic := app.String(cli.StringOpt{
		Name:   "producerTopic",
		Value:  "ConceptAnnotations",
		Desc:   "The topic to write the concept annotation to",
		EnvVar: "PRODUCER_TOPIC",
	})

	log := logger.NewUPPLogger(appSystemCode, *logLevel)

	app.Action = func() {
		log.Infof("System code: %s, App Name: %s, Port: %s", appSystemCode, appName, *port)

		whitelist, regexErr := regexp.Compile(*whitelistRegex)
		if regexErr != nil {
			log.WithError(regexErr).Error("Please specify a valid whitelist ")
		}

		producerConfig := kafka.ProducerConfig{
			BrokersConnectionString: *kafkaAddress,
			Topic:                   *producerTopic,
			Options:                 kafka.DefaultProducerOptions(),
		}
		messageProducer := kafka.NewProducer(producerConfig, log)
		defer func() {
			log.Info("Shutting down kafka producer")
			messageProducer.Close()
		}()

		mapper := service.NewAnnotationMapperService(whitelist, messageProducer, log)

		kafkaConsumerTopic := []*kafka.Topic{
			kafka.NewTopic(*consumerTopic, kafka.WithLagTolerance(int64(*kafkaLagTolerance))),
		}

		consumerConfig := kafka.ConsumerConfig{
			BrokersConnectionString: *kafkaAddress,
			ConsumerGroup:           *consumerGroup,
			Options:                 kafka.DefaultConsumerOptions(),
		}
		messageConsumer := kafka.NewConsumer(consumerConfig, kafkaConsumerTopic, log)

		go messageConsumer.Start(mapper.HandleMessage)
		defer func() {
			log.Info("Shutting down kafka consumer")
			messageConsumer.Close()
		}()

		healthService := health.NewHealthCheck(appSystemCode, appName, appDescription, regexErr, messageConsumer, messageProducer)

		go serveEndpoints(*port, healthService, log)

		waitForSignal()
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}

func serveEndpoints(port string, healthService *health.HealthCheck, log *logger.UPPLogger) {
	serveMux := http.NewServeMux()

	hc := fthealth.TimedHealthCheck{
		HealthCheck: fthealth.HealthCheck{
			SystemCode:  appSystemCode,
			Name:        appName,
			Description: appDescription,
			Checks:      healthService.Checks(),
		},
		Timeout: 10 * time.Second,
	}

	serveMux.HandleFunc(health.HealthPath, fthealth.Handler(hc))
	serveMux.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.GTG))
	serveMux.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	server := &http.Server{Addr: ":" + port, Handler: serveMux}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Infof("HTTP server closing with message: %v", err)
		}
		wg.Done()
	}()

	waitForSignal()
	log.Infof("[Shutdown] pac-annotations-mapper is shutting down")

	if err := server.Close(); err != nil {
		log.Errorf("Unable to stop http server: %v", err)
	}

	wg.Wait()
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
