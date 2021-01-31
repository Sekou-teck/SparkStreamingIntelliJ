
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import sparkBigData._
import java.util._

object spark_DBPostgre {

  def main(args : Array[String]):Unit = {

    val ss = sissionSpark(true)
    val props_postgre = new Properties()
    props_postgre.put("user", "consultant")
    props_postgre.put("password", "pwd#86")
    val df_postgre = ss.read.jdbc("jdbc:postgre://127.0.0.1:3306/jea_db", "orders",props_postgre)

    val df_postgre2 = ss.read
      .format("jdbc")
      .option("url", "jdbc:postgre://127.0.0.1:3306/jea_db")
      .option("user", "consultant")
      .option("password", "pwd#86")
      .option("dbtable", "(select state, city, sum(round(numunits * totalprice)) as commandes totales from orders group by state, city) table_postgreSQL")
      .load()

    // option("query", "select state, city, sum(round(numunits * totalprice)) as commandes totales from orders group by state, city")

    // Une erreur est survenue car Postgre ne supporte pas l'option query,
    // il faut le remplacer par dbtable et la requête select par une table

  df_postgre2.show(20)

    }


}
