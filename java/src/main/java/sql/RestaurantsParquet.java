package sql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

public class RestaurantsParquet {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Restaurants Parquet");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaHiveContext hiveCtx = new JavaHiveContext(sc);

        JavaSchemaRDD inputs = hiveCtx.parquetFile(args[0]);
        inputs.registerTempTable("restaurants");

        hiveCtx.registerFunction("LEN", new UDF1<String, Integer>() {
            public Integer call(String s) throws Exception {
                return s.length();
            }
        }, DataType.IntegerType);

        hiveCtx.sql("CREATE TEMPORARY FUNCTION strLen AS 'sql.StrLen'");

        System.out.println("### Schema ###");
        inputs.printSchema();
        System.out.println();

        System.out.println("### Restaurants in Tokyo ###");
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append("    r.id, ");
        sql.append("    r.alphabet, ");
        sql.append("    LEN(r.alphabet), ");
        sql.append("    strLen(r.alphabet) ");
        sql.append("FROM ");
        sql.append("    restaurants r ");
        sql.append("WHERE ");
        sql.append("    r.pref_id = '13' ");
        sql.append("AND r.alphabet <> '' ");
        sql.append("LIMIT ");
        sql.append("    10 ");

        List<Row> restaurantsInTokyo = hiveCtx.sql(sql.toString()).collect();
        for (Row row : restaurantsInTokyo) {
            System.out.println(row.getString(0) + " " + row.getString(1) + " " + row.getInt(2) + " " + row.getInt(3));
        }
    }
}
