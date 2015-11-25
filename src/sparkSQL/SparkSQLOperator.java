package sparkSQL;

import java.util.ArrayList;
import java.util.List;
 
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;
 
 
/**
 * ע�⣺
 * ʹ��JavaHiveContextʱ
 * 1:��Ҫ��classpath�����������������ļ���hive-site.xml,core-site.xml,hdfs-site.xml
 * 2:��Ҫ����postgresql��mysql������������
 * 3:��Ҫ����hive-jdbc,hive-exec������
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
 
    //����spark sql��ѯhive����ı�
    public static void testHive(JavaHiveContext hiveCtx) {
        List<Row> result = hiveCtx.sql("SELECT * from dy.sparkSQLTest").collect();
        for (Row row : result) {
            System.out.println(row.getString(0) + "," + row.getString(1));
        }
    }
}