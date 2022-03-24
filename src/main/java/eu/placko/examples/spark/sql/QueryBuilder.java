package eu.placko.examples.spark.sql;

public class QueryBuilder {
	private Long limit = 0L;

	public QueryBuilder setLimit(Long limit) {
		this.limit = limit;
		return this;
	}

	public String build() {
		String sql = "SELECT * FROM parquet_schema_evolution.parquet_merge";
		if (limit != null && limit.longValue() > 0) {
			sql = sql + " LIMIT " + limit;
		}
		return sql;
	}
}