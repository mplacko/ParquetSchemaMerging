package eu.placko.examples.spark.app;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import eu.placko.examples.spark.config.SparkConfig;
import eu.placko.examples.spark.config.SparkSQLConfig;
import eu.placko.examples.spark.sql.HiveBuilder;
import eu.placko.examples.spark.sql.QueryBuilder;
import eu.placko.examples.spark.datafactory.*;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;


public class ParquetSchemaMerging {
	private final static Logger LOGGER = Logger.getLogger(ParquetSchemaMerging.class.getName());
	
	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
		    LOGGER.info("wrong input! /user/<REPLACE>/ParquetSchemaMerging/");
		    return;
		}
		String path = args[0];
		
		SparkSession sparkSession = SparkConfig.createSparkSession();
		LOGGER.info("Started Spark Session");	
		
		
		SchemaVersion1 row1V1 = new SchemaVersion1();
		row1V1.setId(1);
		row1V1.setCol_A("A1");
		SchemaVersion1 row2V1 = new SchemaVersion1();
		row2V1.setId(2);
		row2V1.setCol_A("A2");
		
		List<SchemaVersion1> dataV1 = new ArrayList<SchemaVersion1>();
		dataV1.add(row1V1);
		dataV1.add(row2V1);
		
		Dataset<Row> dsV1 = sparkSession.createDataFrame(dataV1, SchemaVersion1.class);
		dsV1.show();
		dsV1.printSchema();
		dsV1
			.write()
			.format("parquet")
			.mode(SaveMode.Overwrite)
			.save(path + "data/partition-date=2022-01-01");
		LOGGER.info("Created dsV1");
		
		
		SchemaVersion2 row1V2 = new SchemaVersion2();
		row1V2.setId(1);
		row1V2.setCol_B("B1");
		row1V2.setCol_A("A1");
		SchemaVersion2 row2V2 = new SchemaVersion2();
		row2V2.setId(2);
		row2V2.setCol_B("B2");
		row2V2.setCol_A("A2");
		
		List<SchemaVersion2> dataV2 = new ArrayList<SchemaVersion2>();
		dataV2.add(row1V2);
		dataV2.add(row2V2);
		
		Dataset<Row> dsV2 = sparkSession.createDataFrame(dataV2, SchemaVersion2.class);
		dsV2.show();
		dsV2.printSchema();
		dsV2
			.write()
			.format("parquet")
			.mode(SaveMode.Overwrite)
			.save(path + "data/partition-date=2022-01-02");
		LOGGER.info("Created dsV2");
		
		
		SchemaVersion3 row1V3 = new SchemaVersion3();
		row1V3.setId(1);
		row1V3.setCol_B("B1");
		SchemaVersion3 row2V3 = new SchemaVersion3();
		row2V3.setId(2);
		row2V3.setCol_B("B2");
		
		List<SchemaVersion3> dataV3 = new ArrayList<SchemaVersion3>();
		dataV3.add(row1V3);
		dataV3.add(row2V3);
		
		Dataset<Row> dsV3 = sparkSession.createDataFrame(dataV3, SchemaVersion3.class);
		dsV3.show();
		dsV3.printSchema();
		dsV3
			.write()
			.format("parquet")
			.mode(SaveMode.Overwrite)
			.save(path + "data/partition-date=2022-01-03");
		LOGGER.info("Created dsV3");
		
		
		Dataset<Row> dsMergeSchema = sparkSession.read()
				.option("mergeSchema","true")
				.parquet(path + "data");
		dsMergeSchema.show();
		dsMergeSchema.printSchema();
		LOGGER.info("Read dsMergeSchema");
		
		Dataset<Row> dsWithoutMergeSchema = sparkSession.read()
				//.option("mergeSchema","false")
				.parquet(path + "data");
		dsWithoutMergeSchema.show();
		dsWithoutMergeSchema.printSchema();
		LOGGER.info("Read dsWithoutMergeSchema");
		
		Dataset<Row> dsMergeSchemaSparkSQL = eu.placko.examples.spark.config.SparkSQLConfig.createSparkSQLSession().read()
				.parquet(path + "data");
		dsMergeSchemaSparkSQL.show();
		dsMergeSchemaSparkSQL.printSchema();
		LOGGER.info("Read dsMergeSchemaSparkSQL");
		
		
		String hive = new HiveBuilder().buildDB();
		sparkSession.sql(hive);
		LOGGER.info("Created Hive DB if not exists");
		
		hive = new HiveBuilder().buildTB();
		sparkSession.sql(hive);
		LOGGER.info("Dropped Hive table if exists");
		
		hive = new HiveBuilder().buildTB(path);
		sparkSession.sql(hive);
		LOGGER.info("Created Hive table over the parquet");
		
		
		hive = new QueryBuilder().build();
		Dataset<Row> dsReadFromHiveTableBeforeRepair = sparkSession.sql(hive);
		dsReadFromHiveTableBeforeRepair.show();
		dsReadFromHiveTableBeforeRepair.printSchema();
		LOGGER.info("Read dsReadFromHiveTableBeforeRepair");
		
		hive = new HiveBuilder().repairTB();
		sparkSession.sql(hive);
		LOGGER.info("MSCK REPAIR TABLE");
		
		hive = new QueryBuilder().build();
		Dataset<Row> dsReadFromHiveTableAfterRepair = sparkSession.sql(hive);
		dsReadFromHiveTableAfterRepair.show();
		dsReadFromHiveTableAfterRepair.printSchema();
		LOGGER.info("Read dsReadFromHiveTableAfterRepair");
	}
}