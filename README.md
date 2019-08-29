# ProgressiveDB

ProgressiveDB was presented in a demo at VLDB 2019 as a middleware to support interactive data exploration. It provides ProgressiveSQL which introduces Progressive Queries and Progressive Views. A Progressive Query returns a fast result that is refined with continuing time. A Progressive View provides some query steering possibilities by mentioning possible future queries.

ProgressiveDB connects via JDBC to a DBMS and provides a JDBC interface to connect to. Currently PostgreSQL and MySQL are supported as data source, however it is possible to add drivers for other database systems. 

# Setup
You can either embed ProgressiveDB into your App or download the standalone version from <https://github.com/DataManagementLab/progressiveDB/releases>. The embedded version is provided via:
```
<dependency>
  <groupId>de.tuda.progressive.db</groupId>
  <artifactId>progressive-db-core</artifactId>
  <version>0.1</version>
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
```
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
```
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
```
server.start();
```

## Connect
Your client application needs to import Apache Calcite Avatica as JDBC driver:
```
<dependency>
  <groupId>org.apache.calcite.avatica</groupId>
  <artifactId>avatica-core</artifactId>
  <version>1.15</version>
</dependency>
```

A connection to ProgressiveDB can be established via:
```
DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9000");
```

## Prepare
To execute Progressive Queries and Progressive Views you need to prepare the table you want to be progressively queried. Joins are basically supported, but will not be covered and explained here, since it is still under research.Therefore, ProgressiveDB should be used with a single table currently.

ProgressiveDB needs the permission to create new tables in the respective databases schema to prepare a table. Use the following statment to prepare a table `test`:
```
try (Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9000")) {
  try (Statement statement = connection.createStatement()) {
    statement.execute("PREPARE TABLE test");
  }
}
```

## Progressive Query

## Progressive View

## Example
