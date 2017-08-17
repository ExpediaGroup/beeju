## Unreleased
### Changed 
* Upgrade to Hive-2.1.1, required a switch from HsqlDB to Derby (Hive no longer seems to support HsqlDB)

## [1.0.2]
### Changed 
* Upgrade parent POM to 1.1.1

## [1.0.1]
### Addded 
* Addition of `HiveServer2JUnitRule` rule to test against Hive Metastore using the JDBC API.

## [1.0.0]
### Added
* First release: `HiveMetaStoreJUnitRule` and `ThriftHiveMetaStoreJUnitRule` rules to test a Hive Metastore connecting directly to the database and the Thrift API respectively.
