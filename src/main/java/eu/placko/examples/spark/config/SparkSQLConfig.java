package eu.placko.examples.spark.config;

import org.apache.spark.sql.SparkSession;
//import java.io.File;

public class SparkSQLConfig {
	public static SparkSession createSparkSQLSession() {
		//String hiveDBLocation = new File("parquet_schema_evolution").getAbsolutePath();
		SparkSession spark = SparkSession
				.builder()
				.appName("ParquetSchemaMerging")
				//.master("local[1]")
				//.master("yarn")
				.config("spark.sql.broadcastTimeout","36000")
				.config("spark.sql.parquet.mergeSchema", "true") // Use Spark SQL
				//.config("spark.sql.warehouse.dir", hiveDBLocation)
				.enableHiveSupport()
				.getOrCreate();
		return spark;
	}
}