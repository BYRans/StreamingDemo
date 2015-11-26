package sparkSQL;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/*park-submit --class sparkSQL.SparkSQLOperator --master  /home/dingyu/sparkSQL.jar*/

public class SparkSQLOperator {
	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf();
		conf.setAppName("SQL Executor");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		HiveContext sqlContext = new HiveContext(jsc.sc());
		String finalSqlQueryStr = "select * from dy.sparksqltest d join xdf.sparksql x where x.xid = d.id";
		Row[] results = sqlContext.sql(finalSqlQueryStr).collect();
		for (Row row : results) {
			System.out.println(row.getString(0) + "\t" + row.getString(1));
		}
		sqlContext.sql("create table dy.nice as "+finalSqlQueryStr);
		jsc.stop();
	}

}
