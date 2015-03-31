package sql

import org.apache.hadoop.hive.ql.exec.UDF

class StrLen extends UDF {

  @Override
  def evaluate(str: String): Int = {
    str.length
  }
}
