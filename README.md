# ProgressiveDB

ProgressiveDB was presented as a demo at [VLDB 2019](http://www.vldb.org/pvldb/vol12/p1814-berg.pdf) as a middleware to support interactive data exploration. It provides ProgressiveSQL which introduces Progressive Queries and Progressive Views. A Progressive Query returns a fast result that is refined with continuing time. A Progressive View provides some query steering possibilities by mentioning possible future queries.

ProgressiveDB connects via JDBC to a DBMS and provides a JDBC interface to connect to. Currently PostgreSQL and MySQL are supported as data source, however it is possible to add drivers for other database systems. 

# Demo

The demo setup can be run in a docker container. The original demo did not use a partitioned table for the exact execution. However, the provided docker image uses for both progressive and exact execution a partitioned table to save memory. Therefore the exact execution will probably much faster. The container can be started via the following command:

```shell script
docker run --name progressive-db-demo -p 5555:5432 -p 8000:8000 -p 8081:8081 -p 9001:9001 -d -e POSTGRES_DB=progressive -e PGDATA=/postgres bergic/progressive-db-demo:1.0
```

After starting the container the demo can be accessed via <http://localhost:8000/index.html>. The underlying PostgreSQL database can be accessed via `psql`:
 ```shell script
sudo -u psql -d progressive -h localhost -p 5555
```

You can also connect to the ProgressiveDB server. Include the following dependency into your project:

```xml
<dependency>
    <groupId>org.apache.calcite.avatica</groupId>
    <artifactId>avatica-core</artifactId>
    <version>1.15.0</version>
</dependency>
```
And connect via:
```java
try (Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9001")) {
    ...
}
```
Continue reading to get to know how ProgressiveDB can be used.

# Setup
You can either embed ProgressiveDB into your App or download the standalone version from <https://github.com/DataManagementLab/progressiveDB/releases>. The embedded version must be compiled first on your own:
```shell script
git clone git@github.com:DataManagementLab/progressiveDB.git
cd progressiveDB
mvn clean install -DskipTests
```
You are now able to include the core into your project:
```xml
<dependency>
  <groupId>de.tuda.progressive.db</groupId>
  <artifactId>progressive-db-core</artifactId>
  <version>0.2-SNAPSHOT</version>
</dependency>
```

## Configuration
### Standalone
If using the standalone version you need to create a configuration file `progressive-db.conf` in the same directory or pass a custom path as first parameter to the jar.

ProgressiveDB needs three different databases to store date called `source`, `meta` and `tmp`. The actual data is stored in `source`, `meta` is used as a storage for needed meta data and `tmp` is used for caching results. We recommend using SQLite for `meta` and `tmp`, while the later one should be in memory. All parameters can be found in the following table:

We recommend setting a fixed `chunkedSize`, since this is still under research. If a value < 0 is used, ProgressiveDB will trie to figure out an appropriate value. The value mainly influences the time till the first result is returned. The lower the value, the faster the first result will be returned and refined results are returned more frequently.

| property        | optional | default |
|-----------------|----------|---------|
| source.url      | false    |         | 
| source.user     | true     | `null`  | 
| source.password | true     | `null`  |
| meta.url        | false    |         | 
| meta.user       | true     | `null`  | 
| meta.password   | true     | `null`  |
| tmp.url         | false    |         | 
| tmp.user        | true     | `null`  | 
| tmp.password    | true     | `null`  |
| port            | true     | `9000`  |
| chunkSize       | true     | `-1`    |

#### Example
```properties
source.url=jdbc:postgresql://localhost:5432/progressive
source.user=postgres
source.password=postgres
meta.url=jdbc:sqlite:progressivedb.sqlite
tmp.url=jdbc:sqlite::memory:
chunkSize=100000
```

### Embedded
If you are using the embedded version you new to create an instance of `de.tuda.progressive.db.ProgressiveDbServer`.

#### Example
```java
final ProgressiveDbServer server = new ProgressiveDbServer.Builder()
  .source(
    "jdbc:postgresql://localhost:5432/progressive",
    "posgres",
    "postgres"
  )
  .meta("jdbc:sqlite:progressivedb.sqlite")
  .tmp("jdbc:sqlite::memory")
  .chunkSize(100000)
  .build();
```
# Run
Run the standalone version by:
```
java -jar progressive-db.jar
```

and the embedded version by:
```java
server.start();
```

## Connect
Your client application needs to import Apache Calcite Avatica as JDBC driver:
```xml
<dependency>
  <groupId>org.apache.calcite.avatica</groupId>
  <artifactId>avatica-core</artifactId>
  <version>1.15</version>
</dependency>
```

A connection to ProgressiveDB can be established via:
```java
DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9000");
```

## Prepare
To execute Progressive Queries and Progressive Views you need to prepare the table you want to be progressively queried. Joins are basically supported, but will not be covered and explained here, since it is still under research. Therefore, ProgressiveDB should be used with a single table currently.

ProgressiveDB needs the permission to create new tables in the respective databases schema to prepare a table. Use the following statment to prepare a table `test`:
```java
try (Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9000")) {
  try (Statement statement = connection.createStatement()) {
    statement.execute("PREPARE TABLE test");
  }
}
```

With a `chunkSize < 0` ProgressiveDB will try to determine an appropriate size. This is still under research, therefore we recommend to set a fixed size.

## Progressive Query
A Progressive Query can be run on a prepared table. It will provide a stream of results that can be accessed via a `ResultSet`. For each processed chunk the results are provided. The `ResultSet` will return `false` for `next` when all chunks are processed. To determine if a new chunk was processed, the functions `progressive_progress` and `progressive_partition` can be used.

## Example
Assuming you started ProgressiveDB with an exemplary configuration with a chunk size of:
```properties
source.url=...
source.user=...
source.password=...
meta.url=jdbc:sqlite:progressivedb.sqlite
tmp.url=jdbc:sqlite::memory:
chunkSize=2
port=9000
```

The following code will produce the later results:

```java
try (Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9000")) {
  try (Statement statement = connection.createStatement()) {
    statement.execute("CREATE TABLE test (a INT, b VARCHAR(100))");
    statement.execute("INSERT INTO test VALUES (1, 'a')");
    statement.execute("INSERT INTO test VALUES (2, 'a')");
    statement.execute("INSERT INTO test VALUES (3, 'b')");
    statement.execute("INSERT INTO test VALUES (4, 'b')");
    statement.execute("PREPARE TABLE test");

    try (ResultSet result = statement.executeQuery("SELECT PROGRESSIVE AVG(a), b, PROGRESSIVE_PROGRESS() from test GROUP BY b")) {
      while (result.next()) {
        final double avg = result.getDouble(1);
        final String group = result.getString(2);
        final double progress = result.getDouble(3);

        System.out.printf("%f | %s | %f\n", avg, group, progress);
      }
    }
  }
}
```

```
2.000000 | a | 0.500000
4.000000 | b | 0.500000
1.500000 | a | 1.000000
3.500000 | b | 1.000000
```

## Progressive View

A Progressive View provides futures to be used by upcoming queries. A Progressive View can be queried via a Progressive Query and is filled chunk per chunk like a Progressive Query.

Futures are possible for conditions (`... WHERE (a > 1) FUTURE AND (a < 5) FUTURE`) and groupings (`SELECT a, b FUTURE FROM ... GROUP BY b FUTURE`). They can be activated via the keyword `WITH FUTURE` for conditions (`WITH FUTURE WHERE a > 1, a < 5`) and groupings (`WITH FUTURE GROUP BY b`). Within a Progressive View definitions futures and immediate conditions and groupings can be mixed.

## Example
```java
try (Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9000")) {
  try (Statement statement = connection.createStatement()) {
    statement.execute("CREATE TABLE test2 (a INT, b VARCHAR(100))");
    statement.execute("INSERT INTO test2 VALUES (1, 'a')");
    statement.execute("INSERT INTO test2 VALUES (2, 'a')");
    statement.execute("INSERT INTO test2 VALUES (3, 'b')");
    statement.execute("INSERT INTO test2 VALUES (4, 'b')");
    statement.execute("PREPARE TABLE test2");

    statement.execute("CREATE PROGRESSIVE VIEW pv AS SELECT AVG(a), b FUTURE, PROGRESSIVE_PROGRESS() FROM test2 WHERE (b = 'a') FUTURE OR (b = 'b') FUTURE GROUP BY b FUTURE");

    try (ResultSet result = statement.executeQuery("SELECT PROGRESSIVE * from pv WITH FUTURE WHERE b = 'b'")) {
      while (result.next()) {
        final double avg = result.getDouble(1);
        final double progress = result.getDouble(2);

        System.out.printf("%f | %f\n", avg, progress);
      }
    }

    try (ResultSet result = statement.executeQuery("SELECT PROGRESSIVE * from pv WITH FUTURE GROUP BY b")) {
      while (result.next()) {
        final double avg = result.getDouble(1);
        final String group = result.getString(2);
        final double progress = result.getDouble(3);

        System.out.printf("%f | %s | %f\n", avg, group, progress);
      }
    }
  }
}
```

## Functions
ProgressiveDB provides some functions that are helpful for progressive execution:

| name                   | param                                         | result                               |
|------------------------|-----------------------------------------------|--------------------------------------|
| progressive_progress   |                                               | percentage of already processed data | 
| progressive_partition  |                                               | last processed chunk                 | 
| progressive_confidence | aggregation column (will probably be changed) | +/- confidence of the actual value   |
