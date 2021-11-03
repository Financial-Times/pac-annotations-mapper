# UPP - PAC Annotations Mapper

Processes content annotations into UPP from TagMe via PAC.

## Primary URL

<https://upp-prod-delivery-glb.upp.ft.com/__pac-annotations-mapper/>

## Service Tier

Platinum

## Lifecycle Stage

Production

## Host Platform

AWS

## Architecture

Mapper for transforming PAC annotations to UPP annotations:

- Reads PAC metadata for an article from the Kafka topic *NativeCmsMetadataPublicationEvents*
- Metadata for sources different than `http://cmdb.ft.com/systems/pac` is skipped by this mapper
- Filters and transforms it to UPP standard JSON representation
- Writes the result onto the Kafka topic *ConceptAnnotations*

This service has NO service endpoints.

## Contains Personal Data

No

## Contains Sensitive Data

No

## Failover Architecture Type

ActiveActive

## Failover Process Type

FullyAutomated

## Failback Process Type

FullyAutomated

## Failover Details

The service is deployed in both Delivery clusters. The failover guide for the cluster is located here:
<https://github.com/Financial-Times/upp-docs/tree/master/failover-guides/delivery-cluster>

## Data Recovery Process Type

NotApplicable

## Data Recovery Details

The service does not store data, so it does not require any data recovery steps.

## Release Process Type

PartiallyAutomated

## Rollback Process Type

Manual

## Release Details

Manual failover is not needed when a new version of the service is deployed to production.

## Key Management Process Type

Manual

## Key Management Details

To access the service clients need to provide basic auth credentials.
To rotate credentials you need to login to a particular cluster and update varnish-auth secrets.

## Monitoring

Service in UPP K8S delivery clusters:

- Delivery-Prod-EU health: <https://upp-prod-delivery-eu.upp.ft.com/__health/__pods-health?service-name=pac-annotations-mapper>
- Delivery-Prod-US health: <https://upp-prod-delivery-us.upp.ft.com/__health/__pods-health?service-name=pac-annotations-mapper>

## First Line Troubleshooting

<https://github.com/Financial-Times/upp-docs/tree/master/guides/ops/first-line-troubleshooting>

## Second Line Troubleshooting

Please refer to the GitHub repository README for troubleshooting information.
