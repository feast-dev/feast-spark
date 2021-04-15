# Changelog

## [v0.2.0](https://github.com/feast-dev/feast-spark/tree/v0.2.0) (2021-04-15)

[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.1.2...v0.2.0)

**Implemented enhancements:**

- Cassandra as sink option for ingestion job [\#51](https://github.com/feast-dev/feast-spark/pull/51) ([khorshuheng](https://github.com/khorshuheng))
- Bigtable as alternative Online Storage [\#46](https://github.com/feast-dev/feast-spark/pull/46) ([pyalex](https://github.com/pyalex))

**Fixed bugs:**

- Cassandra schema update should not fail when column exist [\#55](https://github.com/feast-dev/feast-spark/pull/55) ([khorshuheng](https://github.com/khorshuheng))
- Fix avro serialization: reuse generated schema in advance [\#52](https://github.com/feast-dev/feast-spark/pull/52) ([pyalex](https://github.com/pyalex))

**Merged pull requests:**

- Fix dependencies for aws e2e [\#56](https://github.com/feast-dev/feast-spark/pull/56) ([pyalex](https://github.com/pyalex))
- Better handling for invalid proto messages [\#54](https://github.com/feast-dev/feast-spark/pull/54) ([pyalex](https://github.com/pyalex))
- Bump feast version to 0.9.5 [\#53](https://github.com/feast-dev/feast-spark/pull/53) ([terryyylim](https://github.com/terryyylim))
- Fix setup.py dependencies [\#49](https://github.com/feast-dev/feast-spark/pull/49) ([woop](https://github.com/woop))
- Add documentation building for readthedocs.org [\#47](https://github.com/feast-dev/feast-spark/pull/47) ([woop](https://github.com/woop))
- Cleanup Scala code [\#28](https://github.com/feast-dev/feast-spark/pull/28) ([YikSanChan](https://github.com/YikSanChan))

## [v0.1.2](https://github.com/feast-dev/feast-spark/tree/v0.1.2) (2021-03-17)

[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.1.1...v0.1.2)

**Implemented enhancements:**

- Implicit type conversion for entity and feature table source [\#43](https://github.com/feast-dev/feast-spark/pull/43) ([khorshuheng](https://github.com/khorshuheng))

**Merged pull requests:**

- Make field mapping for batch source consistent with streaming source [\#45](https://github.com/feast-dev/feast-spark/pull/45) ([khorshuheng](https://github.com/khorshuheng))
- Better cleanup for cached rdds in Streaming Ingestion [\#44](https://github.com/feast-dev/feast-spark/pull/44) ([pyalex](https://github.com/pyalex))
- Add tests for master branch [\#25](https://github.com/feast-dev/feast-spark/pull/25) ([woop](https://github.com/woop))

## [v0.1.1](https://github.com/feast-dev/feast-spark/tree/v0.1.1) (2021-03-11)

[Full Changelog](https://github.com/feast-dev/feast-spark/compare/v0.1.0...v0.1.1)

## [v0.1.0](https://github.com/feast-dev/feast-spark/tree/v0.1.0) (2021-03-08)

[Full Changelog](https://github.com/feast-dev/feast-spark/compare/92043a1f67043f531ab76ac2093d0ca604afd825...v0.1.0)

**Fixed bugs:**

- Fix GH Actions PR Workflow Support for PRs from External Contributors [\#35](https://github.com/feast-dev/feast-spark/pull/35) ([mrzzy](https://github.com/mrzzy))
- Allow only project filter when listing ft [\#32](https://github.com/feast-dev/feast-spark/pull/32) ([terryyylim](https://github.com/terryyylim))


**Implemented enhancements:**

- Configurable retry for failed jobs \(JobService Control Loop\) [\#29](https://github.com/feast-dev/feast-spark/pull/29) ([pyalex](https://github.com/pyalex))


**Merged pull requests:**

- Add Helm chart publishing for Feast Spark [\#24](https://github.com/feast-dev/feast-spark/pull/24) ([woop](https://github.com/woop))
- Add project filter to list jobs [\#16](https://github.com/feast-dev/feast-spark/pull/16) ([terryyylim](https://github.com/terryyylim))

- Add protos and update CI [\#23](https://github.com/feast-dev/feast-spark/pull/23) ([pyalex](https://github.com/pyalex))
- Add SparkClient as separate pytest fixture [\#22](https://github.com/feast-dev/feast-spark/pull/22) ([pyalex](https://github.com/pyalex))
- Update CI workflow [\#19](https://github.com/feast-dev/feast-spark/pull/19) ([terryyylim](https://github.com/terryyylim))
- Update submodule [\#18](https://github.com/feast-dev/feast-spark/pull/18) ([terryyylim](https://github.com/terryyylim))
- Use feast core images from GCR instead of building them ourselves [\#17](https://github.com/feast-dev/feast-spark/pull/17) ([pyalex](https://github.com/pyalex))
- Build & push docker workflow for spark ingestion job [\#13](https://github.com/feast-dev/feast-spark/pull/13) ([pyalex](https://github.com/pyalex))
- Fix Azure e2e tests [\#11](https://github.com/feast-dev/feast-spark/pull/11) ([oavdeev](https://github.com/oavdeev))
- fix submodule ref [\#10](https://github.com/feast-dev/feast-spark/pull/10) ([oavdeev](https://github.com/oavdeev))