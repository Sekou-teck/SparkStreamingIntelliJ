import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import sparkBigData._
import java.util._


object spark_DBMysql {
  def main(args: Array[String]): Unit = {

    val ss = sissionSpark(true)
    val props_mysql = new Properties()
    props_mysql.put("user", "consultant")
    props_mysql.put("password", "pwd#86")
    val df_mysql = ss.read.jdbc("jdbc:mysql://127.0.0.1:3306/jea_db?zeroDateTimeBihavior=CONVERT_TO_NULL&serverTimezone=UTC", "jea_db.orders",props_mysql)

    df_mysql.show(20)
    df_mysql.printSchema()

    val df_mysql1 = ss.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306/jea_db?zeroDateTimeBihavior=CONVERT_TO_NULL&serverTimezone=UTC")
      .option("user", "consultant")
      .option("password", "pwd#86")
      .option("dbtable", "jea_db.orders")
      .load()
    // option("query", "select state, city, sum(round(numunits * totalprice)) as commandes totales from orders group by state, city")

    // Une erreur est survenue car mysql ne supporte pas l'option query,
    // il faut le remplacer par dbtable et la requête select par une table

    df_mysql1.show(5)


  }
}
