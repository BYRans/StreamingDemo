package sparkSQL;

import java.util.ArrayList;
import java.util.List;
 
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;
 
 
/**
 * 注意：
 * 使用JavaHiveContext时
 * 1:需要在classpath下面增加三个配置文件：hive-site.xml,core-site.xml,hdfs-site.xml
 * 2:需要增加postgresql或mysql驱动包的依赖
 * 3:需要增加hive-jdbc,hive-exec的依赖
 *
 */
public class SparkSQLOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("simpledemo").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaSQLContext sqlCtx = new JavaSQLContext(sc);
        JavaHiveContext hiveCtx = new JavaHiveContext(sc);
        testHive(hiveCtx);
        sc.stop();
        sc.close();
    }
 
    //测试spark sql查询hive上面的表
    public static void testHive(JavaHiveContext hiveCtx) {
        List<Row> result = hiveCtx.sql("SELECT * from dy.sparkSQLTest").collect();
        for (Row row : result) {
            System.out.println(row.getString(0) + "," + row.getString(1));
        }
    }
}