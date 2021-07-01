
import sparkBigData._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import scala.sys.env // afin de pouvoir créer une nouvelle configuration



object useCaseBano {

  // lecture du fichier BANO pour voir les schema afin de pouvoir imposer le mien


// schema du fichier csv "full"

  val schema_bano = StructType(Array(
    StructField("id_bano", StringType, false),
    StructField("numero_voie", StringType, false),
    StructField("nom_voie", StringType, false),
    StructField("code_postal", StringType, false),
    StructField("nom_commune", StringType, false),
    StructField("code_source_bano", StringType, false),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true)

  ))
 // Instancier le configuration Hadoop

  val configH = new Configuration()
  val fs = FileSystem.get(configH) // les bases de l'App sont posées
  val chemin_destination = new Path("/Users/sekoubafofana/projectBDS/Sources" )

  // Créer un main

  def main(args: Array[String]): Unit = {

    val ss = sissionSpark(true)

    val df_bano_brut = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", true)
      .schema(schema_bano)
      .csv("/Users/sekoubafofana/projectBDS/BANO/full.csv")
    df_bano_brut.show(10)

    // les transformation sur le fichier

    val df_bano = df_bano_brut
      .withColumn("code_departement", substring(col("code_postal"), 1, 2))
      .withColumn("libelle_source", when(col("code_source_bano") === lit("OSM"), lit("OpenStreetMap"))
      .otherwise(when(col("code_source_bano") === lit("OO"), lit("OpenData"))
      .otherwise(when(col("code_source_bano") === lit("0+O"), lit("OpenData enrichi par OSM"))
      .otherwise(when(col("code_source_bano") === lit("CAD"), lit("Cadastre"))
      .otherwise(when(col("code_source_bano") === lit("C+O"), lit("Cadastre enrichi par OSM")))))))
    df_bano.show(10)


    val df_departement = df_bano.select(col("code_departement"))
      .distinct()
      .filter(col("code_departement").isNotNull)

    val liste_departement = df_bano.select(col("code_departement"))
      .distinct()
      .filter(col("code_departement").isNotNull)
      .collect()
      // la méthode collect récupère les données et les transferer
      // vers le Driver (noeud principal du cluster)
      .map(x => x(0)).toList

    liste_departement.foreach(e => println(e.toString))
    // On est avec "FONCTION" qui renvoit un résultat => ()

    // Tandis que "PROCEDURE", exécute des actions et ou des calculs => {}

// il faut copier le fichier contenant les départements :

    //1ère méthode, Scala clasique

    liste_departement.foreach{
      x => df_bano.filter(col("code_departement") === x.toString) // ==> pour chaque département de DataFrame Bano
        // il faut filtrer en fonction de Code de département et le transformer en String
        .coalesce(1)
        .write
        .format("com.databricks.spark.csv")
        .option("delimiter", ";")
        .option("header", true)
        .mode(SaveMode.Overwrite)
        .csv("/Users/sekoubafofana/projectBDS/BANO/bano" + x.toString)
        // Création des chemins
        // En suite, il faut déplacer ces fichiers (97 départements) en utilisant le fs d'Hadoop
        val chemin_sources = new Path("/Users/sekoubafofana/projectBDS/BANO/bano" + x.toString)
        fs.copyFromLocalFile(chemin_sources, chemin_destination)
    }
/*
    // 2ème méthode, ici on est du distribué, Spark et multiprocess

    df_departement.foreach{
      dep => df_bano.filter(col("code_departement") === dep.toString) // ==> pour chaque département de DataFrame Bano
        // il faut filtrer en fonction de Code de département et le transformer en String
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("delimiter", ";")
        .option("header", true)
        .mode(SaveMode.Overwrite)
        .csv("/Users/sekoubafofana/projectBDS/BANO/bano" + dep.toString)
        // Création des chemins
        // En suite, il faut déplacer ces fichiers (97 départements) en utilisant le fs d'Hadoop
        val chemin_sources = new Path("/Users/sekoubafofana/projectBDS/BANO/bano" + dep.toString)
        fs.copyFromLocalFile(chemin_sources, chemin_destination)
    }
*/

  }

}

