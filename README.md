# akka-persistence-dynamodb

[![CI](https://github.com/j5ik2o/akka-persistence-dynamodb/workflows/CI/badge.svg)](https://github.com/j5ik2o/akka-persistence-dynamodb/actions?query=workflow%3ACI)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.j5ik2o/akka-persistence-dynamodb-journal-v2_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.j5ik2o/akka-persistence-dynamodb-journal-v2_2.13)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Tokei](https://tokei.rs/b1/github/j5ik2o/akka-persistence-dynamodb)](https://github.com/XAMPPRocky/tokei)

`j5ik2o/akka-persistence-dynamodb` is an `akka-persistence` plugin for persist `akka-actor` to AWS DynamoDB. 

The plugin features the following.

- Event sourcing
  - Journal Plugin
  - Snapshot Store Plugin
- Durable state
  - Durable State Store Plugin

NOTE: This plugin is derived from [dnvriend/akka-persistence-jdbc](https://github.com/dnvriend/akka-persistence-jdbc), not [akka/akka-persistence-dynamodb](https://github.com/akka/akka-persistence-dynamodb).

## User's Guide

To use the plugins, please see the [User's Guide](https://akka-persistence-dynamodb.readthedocs.io/en/latest/getting-started.html)

## License

Apache License Version 2.0

This product was made by duplicating or referring to the code of the following products, so Dennis Vriend's license is included in the product code and test code.

- [dnvriend/akka-persistence-jdbc](https://github.com/dnvriend/akka-persistence-jdbc)

---
