<img src="doc/image/query-fingerprinting.jpg">
## Sql-Commons
It's a collection of functions for interacting with DuckDB 
- Connection Pooling 
- Query Parsing/Transformation
- Delta Lake/ Hive Partition pruning based on the query predicate
- Detecting similar queries (https://medium.com/@tanejagagan/ac5e00cb96b5)

## Requirement 
- java 17
- maven 

## Compile project
`./mvnw compile`

## Fingerprinting
Replace all the literals from the where clause of a query and hash the query.
Read more about it at https://medium.com/@tanejagagan/ac5e00cb96b5
- `./mvnw exec:java -Dexec.mainClass="io.github.tanejagagan.sql.commons.Fingerprint"`

<img src="doc/image/tree-transformation.png">

## Delta Lake partition pruning
- `./mvnw exec:java -Dexec.mainClass="io.github.tanejagagan.sql.commons.delta.DeltaLakePartitionPruning"`

## Hive partition pruning
`./mvnw exec:java -Dexec.mainClass="io.github.tanejagagan.sql.commons.HivePartitionPruning"`

## TODO Iceberg partition pruning

## Publish the project
- `export GPG_TTY=$(tty)`
- `./mvnw clean -P release-sign-artifacts -DskipTests deploy`
