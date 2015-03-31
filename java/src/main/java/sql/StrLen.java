package sql;

import org.apache.hadoop.hive.ql.exec.UDF;

public class StrLen extends UDF {

    public int evaluate(String str) {
        return str.length();
    }
}
