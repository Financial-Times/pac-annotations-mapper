[![Circle CI](https://circleci.com/gh/Financial-Times/pac-annotations-mapper.svg?style=shield)](https://circleci.com/gh/Financial-Times/pac-annotations-mapper) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/pac-annotations-mapper/badge.svg)](https://coveralls.io/github/Financial-Times/pac-annotations-mapper)

# PAC Annotations Mapper

Mapper for transforming PAC annotations to UPP annotations

* Reads PAC metadata for an article from the Kafka topic _NativeCmsMetadataPublicationEvents_
	* Metadata for sources different than "http://cmdb.ft.com/systems/pac" is skipped by this mapper
* Filters and transforms it to UPP standard JSON representation
* Writes the result onto the Kafka topic _ConceptAnnotations_

## Installation

Download the source code, the dependencies and build the binary

        go get github.com/Financial-Times/pac-annotations-mapper
        cd $GOPATH/src/github.com/Financial-Times/pac-annotations-mapper
        GO111MODULE=on go build ./...
	# If the folder of the service is not located in the GOPATH you can simply use
        go build ./...

## Running locally

1. To run the unit tests:

```shell
go test ./... -v
```

2. Install to the bin folder:

```shell
go install
```

3. Run the binary (using the `help` flag to see the available optional arguments):

```shell
$GOPATH/bin/pac-annotations-mapper [--help]
```

Options:

	--port="8080"                                           Port to listen on ($APP_PORT)
	--zookeeperAddress="localhost:2181"                     Addresses used by the queue consumer to connect to the queue ($ZOOKEEPER_ADDRESS)
	--consumerGroup="pac-annotations-mapper"                Group used to read the messages from the queue ($CONSUMER_GROUP)
	--consumerTopic="NativeCmsMetadataPublicationEvents"    The topic to read the meassages from ($CONSUMER_TOPIC)
	--whitelistRegex="http://cmdb.ft.com/systems/pac"       The regex to use to filter messages based on Origin-System-Id. ($WHITELIST_REGEX)
	--brokerAddress="localhost:9092"                        Address used by the producer to connect to the queue ($BROKER_ADDRESS)
	--producerTopic="ConceptAnnotations"                    The topic to write the concept annotation to ($PRODUCER_TOPIC)

## Endpoints

This service has __NO__ service endpoints.

## Healthchecks

The healthcheck endpoints are:

	/__gtg

	/__health

	/__build-info
