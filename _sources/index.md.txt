# j5ik2o/akka-persistence-dynamodb

Status of This Document: [WIP]

[![CI](https://github.com/j5ik2o/akka-persistence-dynamodb/workflows/CI/badge.svg)](https://github.com/j5ik2o/akka-persistence-dynamodb/actions?query=workflow%3ACI)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/akka-persistence-dynamodb-journal-v2_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/akka-persistence-dynamodb-journal-v2_2.13)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fj5ik2o%2Fakka-persistence-dynamodb.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fj5ik2o%2Fakka-persistence-dynamodb?ref=badge_shield)


`j5ik2o/akka-persistence-dynamodb` is a plugin for akka-persistence.
You can persist your Actor's state to `AWS DynamoDB` without having to worry about detailed persistence techniques.

The plugin features the following.

- [Event sourcing](https://doc.akka.io/docs/akka/current/typed/index-persistence.html)
  - Journal Plugin
  - Snapshot Store Plugin
- [Durable state](https://doc.akka.io/docs/akka/current/typed/index-persistence-durable-state.html)
  - Durable State Store Plugin


## Sitemap

```{toctree}
---
maxdepth: 2
---
getting-started.md
```
