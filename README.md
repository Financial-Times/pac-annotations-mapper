[![Circle CI](https://circleci.com/gh/Financial-Times/pac-annotations-mapper.svg?style=shield)](https://circleci.com/gh/Financial-Times/pac-annotations-mapper) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/pac-annotations-mapper/badge.svg)](https://coveralls.io/github/Financial-Times/pac-annotations-mapper)

# pac-annotations-mapper
Mapper for transforming PAC annotations to UPP annotations

* Reads PAC metadata for an article from the kafka source topic _NativeCmsMetadataPublicationEvents_

** Metadata from other sources is discarded by this mapper

* Filters and transforms it to UP standard json representation
* Puts the result onto the kafka destination topic _ConceptAnnotations_
