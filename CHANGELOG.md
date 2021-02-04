## [4.0.0] - TBD
### Fixed
- Intermediate temporary folders now cleaned up as part of test lifecycle.
- `HiveMetaStoreCore` performs a null check before attempting to close its metastore client.

### Changed
- Hive version updated to `2.3.8` (was `2.3.7`).
- `hive-exec` dependency changed to use `core` classifier to allow source code retrieval for easier debugging.
- Hive's `METASTORE_CONNECTION_POOLING_TYPE` is now set to `"NONE"`.
- Hive's `HIVESTATSAUTOGATHER` is now set to `false"`.
- Hive's `HIVE_SERVER2_LOGGING_OPERATION_ENABLED` is now set to `false"`.
- Various Derby, scratch and similar folders now configured to use a base temporary folder.
- JUnit4 Rules now extend `TestWatcher` instead of `ExternalResource` to allow cleanup of temporary folders on test failures.
- `BeejuCore` `tempDir()` returns base temporary test folder instead of temporary Hive warehouse dir.
- `BeejuCore` returns temporary Hive warehouse dir via `warehouseDir()`.
- JUnit5 Extensions `getTempDirectory()` returns base temporary test folder instead of temporary Hive warehouse dir.
- JUnit5 Extensions return temporary Hive warehouse dir via `getWarehouseDirectory()`.
- JUnit4 Rules `tempDir()` returns base temporary test folder instead of temporary Hive warehouse dir.
- JUnit4 rules return temporary Hive warehouse dir via `warehouseDir()`.
- `hotels-oss-parent` version updated to `6.1.0` (was `5.0.0`) to enable building with Java 11.

## [3.3.0] - 2020-10-23
### Changed
- Allow passing in of "pre" and "post" configuration values so that BeeJU's defaults can be overridden.
- JUnit version updated to `5.7.0` (was `5.5.2`).
- `hotels-oss-parent` version updated to `5.0.0` (was `4.2.0`). 

## [3.2.0] - 2020-10-14
### Added
- Support for setting Thrift Hive Metastore port in tests.
- A `ThriftHiveMetaStoreApp` which can be used to run the the Thrift Hive Metastore service locally.

### Changed
- Changed visibility of `createDatabase()` method in `BeejuJUnitRule` from default to public (for external usage). 

## [3.1.0] - 2020-05-13
### Changed
- JUnit version updated to `5.5.2` (was `5.5.1`).
- Depend on `junit-jupiter` (was `junit-jupiter-api`).
- `hotels-oss-parent` version updated to `4.2.0` (was `4.1.0`).
- Upgraded version of `hive.version` to `2.3.7` (was `2.3.4`). Allows BeeJU to be used on JDK>=9.

### Added
- Support for setting Hive conf using arbitrary string as conf key.

## [3.0.1] - 2019-09-27
### Changed
- `HiveMetaStoreJUnitExtension` and `HiveServer2JUnitExtension` constructors made public to allow access to classes outside of the extensions package. 

## [3.0.0] - 2019-09-06
### Changed
- JDK version upgrade to 1.8 (was 1.7).

### Added
- JUnit5 extension class equivalents for all BeeJU Rules. 

## [2.0.0] - 2019-09-02
### Added
- Support for JUnit5 using `migration-support` dependency. NOTE - the transitive dependency for JUnit4 from Beeju has been removed so you must depend on it in your own POM.

### Changed
- Excluded `org.pentaho.pentaho-aggdesigner-algorithm` dependency as it's not available in Maven Central.
- `hotels-oss-parent` version updated to 4.1.0 (was 4.0.1). 

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
