package eu.placko.examples.spark.sql;

public class HiveBuilder {
	public String buildDB() {
		String sql = "CREATE DATABASE IF NOT EXISTS parquet_schema_evolution LOCATION '/user/hive/databases/parquet_schema_evolution.db'";
		return sql;
	}
	
	public String buildTB() {
		String sql = "DROP TABLE IF EXISTS parquet_schema_evolution.parquet_merge";
		return sql;
	}
	
	public String buildTB(String path) {
		String sql = "CREATE EXTERNAL TABLE parquet_schema_evolution.parquet_merge (id INT, col_a STRING) PARTITIONED BY (`partition-date` STRING) STORED AS PARQUET LOCATION 'hdfs://nameservice1" + path + "data'";
		return sql;
	}
	
	public String repairTB() {
		String sql = "MSCK REPAIR TABLE parquet_schema_evolution.parquet_merge";
		return sql;
	}
}