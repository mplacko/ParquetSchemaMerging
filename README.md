# ParquetSchemaMerging
An example for explaining schema evolution with Parquet (column based) binary data format. Starting with a simple schema, adding new columns, deleting existing columns and ending up with multiple Parquet files with different but compatible schemas.

### HOW TO CONFIGURE THE PROJECT
- `ParquetSchemaMerging.sh`
```sh
keytabUser=<REPLACE>
keytab=/etc/security/keytabs/<REPLACE>.keytab

appArtifact="/home/<REPLACE>/spark/java/ParquetSchemaMerging-$appVersion.jar /user/<REPLACE>/ParquetSchemaMerging/"
log4j_setting="-Dlog4j.configuration=file:///home/<REPLACE>/spark/java/log4j.xml"
```
- `log4j.xml`
```sh
<param name="file" value="/home/<REPLACE>/spark/java/log.out" />
```

## Building and Running

### Build
To build the application it is required to have this installed:
- `Java 9`
- `Maven 3.x`

Then just run this:
```sh
mvn clean install
```

### Submit to Spark cluster
For running spark application see shell script inside `/scripts` dir.
