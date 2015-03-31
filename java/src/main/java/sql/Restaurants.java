package sql;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

public class Restaurants {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Restaurants");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaHiveContext hiveCtx = new JavaHiveContext(sc);

        JavaRDD<Restaurant> restaurants = sc
            .textFile(args[0])
            .filter(new Function<String, Boolean>() {
                public Boolean call(String l) throws Exception {
                    return !l.startsWith("id");
                }
            })
            .map(new Function<String, String[]>() {
                public String[] call(String l) throws Exception {
                    return l.split(",");
                }
            })
            .filter(new Function<String[], Boolean>() {
                public Boolean call(String[] c) throws Exception {
                    return c.length >= 7;
                }
            })
            .map(new Function<String[], Restaurant>() {
                public Restaurant call(String[] c) throws Exception {
                    return new Restaurant(c[0], c[1], c[2], c[3], c[4], c[5], c[6]);
                }
            });
        hiveCtx.applySchema(restaurants, Restaurant.class).registerTempTable("restaurants");

        JavaRDD<Pref> prefs = sc
            .textFile(args[1])
            .filter(new Function<String, Boolean>() {
                public Boolean call(String l) throws Exception {
                    return !l.startsWith("id");
                }
            })
            .map(new Function<String, String[]>() {
                public String[] call(String l) throws Exception {
                    return l.split(",");
                }
            })
            .filter(new Function<String[], Boolean>() {
                public Boolean call(String[] c) throws Exception {
                    return c.length >= 2;
                }
            })
            .map(new Function<String[], Pref>() {
                public Pref call(String[] c) throws Exception {
                    return new Pref(c[0], c[1]);
                }
            });
        hiveCtx.applySchema(prefs, Pref.class).registerTempTable("prefs");

        JavaRDD<Area> areas = sc
            .textFile(args[2])
            .filter(new Function<String, Boolean>() {
                public Boolean call(String l) throws Exception {
                    return !l.startsWith("id");
                }
            })
            .map(new Function<String, String[]>() {
                public String[] call(String l) throws Exception {
                    return l.split(",");
                }
            })
            .filter(new Function<String[], Boolean>() {
                public Boolean call(String[] c) throws Exception {
                    return c.length >= 3;
                }
            })
            .map(new Function<String[], Area>() {
                public Area call(String[] c) throws Exception {
                    return new Area(c[0], c[1], c[2]);
                }
            });
        hiveCtx.applySchema(areas, Area.class).registerTempTable("areas");

        System.out.println("### Restaurants in Tokyo ###");
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append("    r.alphabet, ");
        sql.append("    p.name ");
        sql.append("FROM ");
        sql.append("    restaurants r ");
        sql.append("INNER JOIN prefs p ");
        sql.append("    ON p.id = r.prefId ");
        sql.append("WHERE ");
        sql.append("    p.id = '13' ");
        sql.append("AND r.alphabet <> '' ");
        sql.append("LIMIT ");
        sql.append("    10 ");

        List<Row> restaurantsInTokyo = hiveCtx.sql(sql.toString()).collect();
        for (Row row : restaurantsInTokyo) {
            System.out.println(row.getString(0) + " " + row.getString(1));
        }
    }

    public static class Restaurant implements Serializable {
        private String id;
        private String name;
        private String property;
        private String alphabet;
        private String nameKana;
        private String prefId;
        private String areaId;

        public Restaurant(String id, String name, String property, String alphabet,
                          String nameKana, String prefId, String areaId) {
            this.id = id;
            this.name = name;
            this.property = property;
            this.alphabet = alphabet;
            this.nameKana = nameKana;
            this.prefId = prefId;
            this.areaId = areaId;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getProperty() {
            return property;
        }

        public void setProperty(String property) {
            this.property = property;
        }

        public String getAlphabet() {
            return alphabet;
        }

        public void setAlphabet(String alphabet) {
            this.alphabet = alphabet;
        }

        public String getNameKana() {
            return nameKana;
        }

        public void setNameKana(String nameKana) {
            this.nameKana = nameKana;
        }

        public String getPrefId() {
            return prefId;
        }

        public void setPrefId(String prefId) {
            this.prefId = prefId;
        }

        public String getAreaId() {
            return areaId;
        }

        public void setAreaId(String areaId) {
            this.areaId = areaId;
        }
    }

    public static class Pref {
        private String id;
        private String name;

        public Pref(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class Area {
        private String id;
        private String prefId;
        private String name;

        public Area(String id, String prefId, String name) {
            this.id = id;
            this.prefId = prefId;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getPrefId() {
            return prefId;
        }

        public void setPrefId(String prefId) {
            this.prefId = prefId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
