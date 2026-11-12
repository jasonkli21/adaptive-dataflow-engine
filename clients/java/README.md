# ADE Flight SQL Java client

Compile with OpenJDK 17 and Maven 3.9.11. The Maven project pins Apache Arrow
Flight SQL 19.0.0. The
[Phase 22 guide](../../docs/phase-22-multilanguage-udf-guide.md) shows classpath
setup, standard SQL and shared UTF-8 JSON plan requests against a live gateway.
`GatewayClient` implements AutoCloseable. Close the FlightStream before the client.
