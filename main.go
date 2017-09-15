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
	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/Financial-Times/pac-annotations-mapper/health"
	"github.com/Financial-Times/pac-annotations-mapper/service"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
)

const (
	appName        = "PAC Annotations Mapper"
	appDescription = "UPP mapper for PAC annotations"
	appSystemCode  = "pac-annotations-mapper"
)

func main() {
	app := cli.App("pac-annotations-mapper", appDescription)

	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})

	// Kafka consumer config
	zookeeperAddress := app.String(cli.StringOpt{
		Name:   "zookeeperAddress",
		Value:  "localhost:2181",
		Desc:   "Addresses used by the queue consumer to connect to the queue",
		EnvVar: "ZOOKEEPER_ADDRESS",
	})
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

	// message filter
	whitelistRegex := app.String(cli.StringOpt{
		Name:   "whitelistRegex",
		Desc:   "The regex to use to filter messages based on Origin-System-Id.",
		EnvVar: "WHITELIST_REGEX",
		Value:  `http://cmdb\.ft\.com/systems/pac`,
	})

	// Kafka producer config
	brokerAddress := app.String(cli.StringOpt{
		Name:   "brokerAddress",
		Value:  "localhost:9092",
		Desc:   "Address used by the producer to connect to the queue",
		EnvVar: "BROKER_ADDRESS",
	})
	producerTopic := app.String(cli.StringOpt{
		Name:   "producerTopic",
		Value:  "ConceptAnnotations",
		Desc:   "The topic to write the concept annotation to",
		EnvVar: "PRODUCER_TOPIC",
	})

	log.SetFormatter(&log.JSONFormatter{TimestampFormat: time.RFC3339Nano})
	log.SetLevel(log.InfoLevel)
	log.Info("[Startup] pac-annotations-mapper is starting ")

	app.Action = func() {
		log.Infof("System code: %s, App Name: %s, Port: %s", appSystemCode, appName, *port)

		whitelist, regexErr := regexp.Compile(*whitelistRegex)
		if regexErr != nil {
			log.Error("Please specify a valid whitelist ", regexErr)
		}

		messageProducer, _ := kafka.NewPerseverantProducer(*brokerAddress, *producerTopic, nil, 0, time.Minute)

		mapper := service.NewAnnotationMapperService(whitelist, messageProducer)

		messageConsumer, _ := kafka.NewPerseverantConsumer(*zookeeperAddress, *consumerGroup, []string{*consumerTopic}, kafka.DefaultConsumerConfig(), 0, time.Minute)

		go serveEndpoints(*port, messageConsumer, messageProducer, regexErr)

		go messageConsumer.StartListening(mapper.HandleMessage)

		waitForSignal()
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}

func serveEndpoints(port string, consumer kafka.Consumer, producer kafka.Producer, whitelistErr error) {
	healthService := health.NewHealthCheck(appSystemCode, appName, appDescription, whitelistErr, consumer, producer)

	serveMux := http.NewServeMux()

	hc := fthealth.HealthCheck{SystemCode: appSystemCode, Name: appName, Description: appDescription, Checks: healthService.Checks()}
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
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
