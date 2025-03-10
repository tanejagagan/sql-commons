<img src="doc/image/query-fingerprinting.jpg">

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
- `./mvnw exec:java -Dexec.mainClass="io.github.tanejagagan.sql.commons.DeltaLakePartitionPruning"`

## Hive partition pruning
`./mvnw exec:java -Dexec.mainClass="info.gtaneja.sql.commons.HivePartitionPruning"`

## TODO Iceberg partition pruning