## [3.0.0] - TBD
### Changed
- JDK version upgrade to 1.8 (was 1.7).

### Added
- JUnit5 extension class equivalents for all Beeju Rules. 

## [2.0.0] - 2019-08-30
### Added
- Support for JUnit5 using `migration-support` dependency. NOTE - the transitive dependency for JUnit from Beeju has been removed so you must depend on it in your own POM. 

## [1.3.2] - 2019-07-10
### Changed
- Release process now uses HTTPS (was SSH) from build slaves to GitHub, no changes to code or functionality.

## [1.3.1] - 2019-04-11
### Changed
- `hotels-oss-parent` version updated to 4.0.1 (was 2.3.5).
- Refactored project to remove checkstyle and findbugs warnings, which does not impact functionality.

## [1.3.0] - 2018-12-18
### Changed
- `log4j-slf4j-impl` transitive dependency excluded. See [#17](https://github.com/HotelsDotCom/beeju/issues/17).
- Hive version upgraded to 2.3.4 (was 2.3.0) and transitive dependencies on HBase which in turn depended on JDK tools 1.7 excluded. See [#19](https://github.com/HotelsDotCom/beeju/issues/19).
- `hotels-oss` parent pom upgraded to 2.3.3 (was 2.0.6). See [#19](https://github.com/HotelsDotCom/beeju/issues/19).

## [1.2.1] - 2017-11-09
### Changed
- Change `ConfVars` added in Hive 2.x to their equivalent string.

## [1.2.0] - 2017-10-03
### Changed
- Upgrade to Hive-2.3.0.
- Upgrade parent POM to 2.0.3.

### Added
- The rules now accept Hive configuration properties at construction time.

## [1.1.3] - 2017-09-25
### Changed
- Depend on latest parent with test.arguments build parameter.

## [1.1.0] - 2017-08-18
### Changed
- Upgrade to Hive-2.1.1, required a switch from HsqlDB to Derby (Hive no longer seems to support HsqlDB).

## [1.0.2]
### Changed
- Upgrade parent POM to 1.1.1.

## [1.0.1]
### Added
- Addition of `HiveServer2JUnitRule` rule to test against Hive Metastore using the JDBC API.

## [1.0.0]
### Added
- First release: `HiveMetaStoreJUnitRule` and `ThriftHiveMetaStoreJUnitRule` rules to test a Hive Metastore connecting directly to the database and the Thrift API respectively.
