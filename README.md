# shs-cli

CLI tool for interacting with the Apache Spark History Server REST API, designed for use by AI agents.

## OpenAPI Spec

The OpenAPI spec (`openapi.yaml`) is manually maintained to match the Spark History Server source code. Response types are defined across three files in the Spark codebase:

| Source file | Types |
|---|---|
| `core/src/main/scala/org/apache/spark/status/api/v1/api.scala` | Application, Job, Stage, Task, Executor, Environment, Version |
| `sql/core/src/main/scala/org/apache/spark/status/api/v1/sql/api.scala` | ExecutionData (SQL), Node, Metric |
| `streaming/src/main/scala/org/apache/spark/status/api/v1/streaming/api.scala` | StreamingStatistics, ReceiverInfo, BatchInfo, OutputOperationInfo |

When adding fields or schemas, check these files as the source of truth. The timestamp format is hardcoded as `yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'` in UTC — see `JacksonMessageWriter.scala`.
