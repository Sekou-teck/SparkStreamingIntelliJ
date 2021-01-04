import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
// importation des functions comme col()
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs._
import org.apache.spark.sql.expressions.UserDefinedFunction


object sparkBigData {
  var ss : SparkSession = null
  var spConf : SparkConf = null

  //private var trace_log : Logger =



  // imposer une structure relationnelle de data Frame


  val schema_order = StructType(Array(
    StructField("orderid", IntegerType, false),
    StructField("customerid", IntegerType, false),
    StructField("campaignid", IntegerType, true),
    StructField("orderdate", TimestampType, true),
    StructField("city", StringType, true),
    StructField("state", StringType, true),
    StructField("zipcode", StringType, true),
    StructField("paymenttype", StringType, true),
    StructField("totalprice", DoubleType, true),
    StructField("numorderlines", IntegerType, true),
    StructField("numunits", IntegerType, true)
  ))

  // df_test_schema :

  val schema_test = StructType(Array(
    StructField("_c0", StringType, false),
    StructField("InvoiceNo", IntegerType, false),
    StructField("StockCode", StringType, true),
    StructField("Description", StringType, true),
    StructField("Quantity", IntegerType, true),
    StructField("InvoiceDate", DateType, true),
    StructField("UnitPrice", DoubleType, true),
    StructField("CustomerID", DoubleType, false),
    StructField("Country", StringType, true),
    StructField("InvoiceTimestamp", TimestampType, true)
  ))

  //df_orderline_schema :

  val schema_orderline = StructType(Array(
    StructField("orderlineid", IntegerType, false),
    StructField("orderid", IntegerType, false),
    StructField("productid", IntegerType, false),
    StructField("shipdate", TimestampType, true),
    StructField("billdate", TimestampType, true),
    StructField("unitprice", DoubleType, true),
    StructField("numunits", IntegerType, true),
    StructField("totalprice", DoubleType, true)
  ))

  // Product_Schema :

  val schema_product = StructType(Array(
    StructField("PRODUCTID", IntegerType, false),
    StructField("PRODUCTNAME", StringType, true),
    StructField("PRODUCTGROUPCODE", StringType, true),
    StructField("PRODUCTGROUPNAME", StringType, true),
    StructField("INSTOCKFLAG", StringType, true),
    StructField("FULLPRICE", IntegerType, true)
  ))


  // Principal Data Frame

  def main(args: Array[String]) : Unit = {


    val session_s =  sissionSpark(true)

    // Pour créer un DataFrame, on peut passer soit par creDataFrame() ou read()
    val df_test = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema_test)
      .csv("/Users/sekoubafofana/projectBDS/dataFrame/sources de données/2010-12-06.csv")
    // csv charge juste un seul fichier

    // df_test.printSchema()
    // df_test.show(15) // lecture

    println()

    val df_gp = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/sekoubafofana/projectBDS/dataFrame/sources de données/*.csv")
    // peut charger plusieurs fichiers à la fois, le load est une transformation
    // df_gp.show(7)
    // println("df_test count : " + df_test.count() + " df_group count : " + df_gp.count()) // comptage

    println()

    val df_gp2 = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/sekoubafofana/projectBDS/dataFrame/sources de données/2010-12-06.csv", "/Users/sekoubafofana/projectBDS/dataFrame/sources de données/2011-12-08.csv")
    // peut charger plusieurs fichiers à la fois
    // df_gp2.show(7)
    // println("df_gp count : " + df_gp.count() + " df_group2 count : " + df_gp2.count()) // comptage


    // Connaitre le schema d'un fichier

    // df_test.printSchema()

    println()

    val df_2 = df_test.select(
      col("InvoiceNo").cast(StringType),
      col("_c0").alias("ID_CLIENT"),
      col("StockCode").cast(IntegerType).alias("Code de la marchandise"),
      // On est dans une logique enssempliste
      col("Invoice".concat("No")).alias("ID de la commande")
    )
    // df_2.show(4)

    // Je peux ne pas créer un dataFrame nouveau comme df_2 pour manipuler les functions col()

    // Je fais ceci :

    //  df_2 = df_test.select(
    //  col("InvoiceNo").cast(IntegerType),
    // col("_c0").alias("ID_CLIENT"),
    //  col("StockCode").cast(IntegerType).alias("Code de la marchandise") // On est dans une logique enssempliste
    // df_2.show(4)

    println()


    // les function comme withColumn et withColmnRename


    val df_3 = df_test.withColumn("InvoiceNo", col("InvoiceNo").cast(StringType))
      .withColumn("StockCode", col("StockCode").cast(IntegerType))
      .withColumn("valeur_constante", lit(50))
      .withColumnRenamed("_c0", "ID_client")
      .withColumn("ID_commande", concat_ws("|", col("InvoiceNo"), col("ID_client")))
      .withColumn("total_amount", round(col("Quantity") * col("UnitPrice"), 2))
      .withColumn("created_dt", current_timestamp())
      .withColumn("Reduction", when(col("total_amount") < 20 , lit(0))
        .otherwise(lit(3)))
      //.orderBy(desc("total_amount"))
      .withColumn("reduction_test", when(col("total_amount") < 15, lit(0))
        .otherwise(when(col("total_amount")
          .between(15, 20), lit(3))
          .otherwise(when(col("total_amount") > 20, lit(4)))))

      .withColumn("net_income", col("total_amount") - col("Reduction"))

    // df_3.show(50)

    println()

    // Les filtres

    val df_not_reduced = df_3.filter(col("reduction")=== lit(0) && col("Country").isin("United Kingdom", "France", "USA"))

    //   df_not_reduced.show(5)

    println()

    // Jointures et aggregations de dataFrame

    // Join :

    val df_orders = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema_order)
      .load("/Users/sekoubafofana/projectBDS/dataFrame/sources de données/orders.txt")


    val df_ordersGood =  df_orders.withColumnRenamed("numunits", "numunits_order")
      .withColumnRenamed("totalprice", "totalprice_order")

    val df_orderline = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema_orderline)
      .load("/Users/sekoubafofana/projectBDS/dataFrame/sources de données/orderline.txt")

    val df_product = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema_product)
      .load("/Users/sekoubafofana/projectBDS/dataFrame/sources de données/product.txt")

    //    df_orders.show(5)
    //    df_orders.printSchema()
    //  df_orderline.show(5)
    //  df_orderline.printSchema()

    // df_product.show(5)
    // df_product.printSchema()

    // df_orders.printSchema()
    // df_orders.show(5)

    val df_joinOrders =  df_orderline.join(df_ordersGood, df_ordersGood.col("orderid") === df_orderline.col("orderid"), "inner")
      .join(df_product, df_product.col("productid") === df_orderline.col("productid"), Inner.sql)
    //  df_joinOrders.printSchema()
    // df_joinOrders.show(5)

    // Union :

    val df_fichier1 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("/Users/sekoubafofana/projectBDS/dataFrame/sources de données/2010-12-06.csv")

    val df_fichier2 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("/Users/sekoubafofana/projectBDS/dataFrame/sources de données/2011-01-20.csv")

    val df_fichier3 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("/Users/sekoubafofana/projectBDS/dataFrame/sources de données/2011-12-08.csv")

    val df_unitedFiles = df_fichier1.union(df_fichier2.union(df_fichier3))

    println(df_fichier3.count() + " " + df_unitedFiles.count())

    // Aggregation :

    df_joinOrders.withColumn("total_amount", round(col("numunits") * col("totalprice"), 3))
      .groupBy("state", "city")
      .sum("total_amount").alias("commandes totales")
    //      .show()

    // Fenêtrage : col("state")

    val wn_epecs = Window.partitionBy(col("state")).orderBy(col("state").desc)
    // sans indiquer ordre, l'affichage sera automatiquement de plus petit au plus grand

    val df_window = df_joinOrders.withColumn("vente_dep", sum(round(col("numunits") * col("totalprice"), 3)).over(wn_epecs))
      .select(col( "orderlineid"),
        col("zipcode"),
        col("state"),
        col("PRODUCTGROUPNAME"),
        col("vente_dep").alias("ventes_par_departement")

      )//.show(10)

    // Manipulation des Temps et dates en Spark :
    /*
        df_ordersGood.withColumn("date_lecture", date_format(current_date(), "dd/MMMM/yyyy hh:mm:ss")) // MMMM => donne le nom complet du mois.
          .withColumn("date_lecture_complet", current_timestamp())
          .withColumn("periodes_seconds", window(col("orderdate"), "5 seconds"))
          .select(
            col("periodes_seconds"),
            col("orderdate.start"),
            col("orderdate.end")
          )
    */
    // Les différentes formes de format de dates :

    df_unitedFiles.withColumn("InvoiceDate", to_date(col("InvoiceDate")))
      .withColumn("InvoiceTimestamp", col("InvoiceTimestamp").cast(TimestampType))
      .withColumn("Invoice_add_2months", add_months(col("InvoiceDate"), 2))
      .withColumn("Invoice_sub_date", date_sub(col("InvoiceDate"), 28))
      .withColumn("Invoice_diff_date", datediff(current_timestamp(), col("InvoiceDate")))
      .withColumn("Invoice_date_quarters", quarter(col("InvoiceDate")))
      .withColumn("Invoice_date_id", unix_timestamp(col("InvoiceDate")))
      .withColumn("Invoice_date_format", from_unixtime(unix_timestamp(col("InvoiceDate")), "dd/MM/yyyy"))
      .show(10)

    // Un exemple de manipulation des textes

    df_product
      .withColumn("productGP", substring(col("PRODUCTGROUPNAME"), 3, 2))
      .withColumn("productln", length(col("PRODUCTGROUPNAME")))
      .withColumn("concatProduct", concat_ws("|", col("PRODUCTID"), col("INSTOCKFLAG")))
      .withColumn("PRODUCTGROUPCODEMIN", lower(col("PRODUCTGROUPCODE")))

      // Contruction des motifs de correspondance des Expressions Régulières pour une validation dans un projet...
      .where(regexp_extract(trim(col("PRODUCTID")), "[0-9]{5}",0) === trim(col("PRODUCTID")))
      .where(!col("PRODUCTID").rlike("[0-9]{5}"))
      //.count()
     // .show(10)

  // UDF : UserDefineFunction :
    // 1) Créer une fonction classique "de base ou Spark.sql.type"  ,
    // 2) définir une fonction udf et
    // 3) mettre tous dans DataFrame

    // 1ère étape :
    def valid_phone(phone_to_test : String) : Boolean = {
      var result : Boolean = false
      val motif_regex = "^0[0-9]{9}".r
        if(motif_regex.findAllIn(phone_to_test.trim) == phone_to_test.trim){
          result = true
        } else {
          result = false
        }
      return result
    }
 // 2ème étape : Définir une fonction comme du type udf.
    // Pour cela, il faut d'abord importer le module Spark.sql.expressions.UserDefineFunction

  val valid_phoneUDF : UserDefinedFunction = udf{(phone_to_test : String) => valid_phone(phone_to_test : String)}

    // 3ème étape : DataFrame

    import session_s.implicits._
    val phone_list : DataFrame = List("0709789486", "+3307897025", "8794007834").toDF("phone_number")
    phone_list
      .withColumn("test_phone", valid_phoneUDF(col("phone_number")))
      .show()

    // Persistance : elle se faite soit en csv, orc, parquet, etc.

    df_window
      .repartition(1)
      // .schema(schema_order) // Cette option nous permets d'imposer une structure au fichier en écriture, elle vient avant write
      .write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/Users/sekoubafofana/projectBDS/dataFrame/sources de données/persistCSV")
    //  .save("/Users/sekoubafofana/projectBDS/dataFrame/sources de données/persist")
    // ici, le format est parquet, il est spécifiquement adapté pour hdfs.


    // Exemple de propriété d'un format :
    /*
        df_2.write
          .option("orc.bloom-filter.column", "favorite_color")
          .option("orc_dictionary.key.threshold", "1.6")
          .option("orc.column.encoding.direct", "name")
          .orc("user_with_options.orc")

    */

    def spark_hdfs () : Unit = {

      val config_fs = sissionSpark(true).sparkContext.hadoopConfiguration
      val fs = FileSystem.get(config_fs)

      val src_path = new Path("/User/datalake/marketing")
      val  dest_path = new Path("/User/datalake/indexes")
      val ren_src = new Path("/User/datalake/marketing/fichier_reporting.parquet") // renommer ce fichier_reporting
      val dest_src = new Path("/User/datalake/marketing/reporting.parquet") // en reporting tout court
      val local_path = new Path("/Users/sekoubafofana/projectBDS/dataFrame/sources de données/persistCSV/part-00000-f1f6d1e6-37d8-4377-b53e-75b35df785b3-c000.csv")
      val path_local = new Path("/Users/sekoubafofana/projectBDS/dataFrame/sources de données")

      // Pour la lire des fichiers du dossier marketing, je fais :

      val  file_list = fs.listStatus(src_path) // 1er procédé de lecture
      file_list.foreach(f => println(f.getPath))

      val file_list1 = fs.listStatus(src_path).map(f1 => f1.getPath) // 2èeme procédé
      for(i <- 1 to file_list1.length) {
        println(file_list1(i))
      }

      // Pour renommer

      fs.rename(ren_src, dest_src) // il sera renommer en reporting.parquet
      // rename copie aussi le fichier d'un répertoire hdfs à l'autre.

      // Suppression d'un fichier :
      fs.delete(dest_src, true)

      // Il va supprimer tous les fichiers contenant ce répertoire,
      // on peut spécifier un fichier en mettant son nom

      // Copier un fichier : // copyFromLocalFile => du disc local d'une machine vers hdfs ou l'inverse, il faut :

      fs.copyFromLocalFile(local_path, dest_path) // Possible avec variabilisation du chémin local

      fs.copyToLocalFile(dest_path, path_local) // L'inverse est possible seulement si on enlève le nom du fichier et son dossier parent
      // il accepte de prendre tout le répertoire et son contenu.


    }



  }


  // Principal pour RDD

  def manip_rdd() : Unit = {


    // Création de SparkContext

    val sc = sissionSpark(env = true).sparkContext
    sc.setLogLevel("OFF") // permet de désactiver les logs

    // Création de SparkSession

    val session_s =  sissionSpark(true)

    // Creation de RDD à partir d'une liste


    val rdd_test : RDD[String] = sc.parallelize(List("Alain", "Sekou", "julien", "anna", "ibrahim"))
    rdd_test.foreach{
      l => println(l)}

    println()

    // Creation de RDD à partir d'un tableau

    val rdd2 : RDD[String] = sc.parallelize(Array("Lucie", "Sekouba", "AÏssta", "Aîcaley", "Ibrahim"))
    rdd2.foreach{ l => println(l)}

    println()
    // Creation à partir d'une séquence

    /* 4! val rdd3 = sc.parallelize(Seq(("Sekouba", "Maths", 16), ("Aissata", "Maths", 14), ("Aicaley", "Maths", 20), ("Ibrahim", "Maths", 19)))
        println("Premier élément de mon RDD 3")
        rdd3.take(1).foreach{ l => println(l)}

    // Une méthode d'affichage des éléments de RDD
        if(rdd3.isEmpty()){
          println("Le RDD est vide")
        } else {
          rdd3.foreach{
            l => println(l)
          }
        }


     */
    // Une autre méthode : enregistrer sur disk

    // 3! rdd3.saveAsObjectFile(path ="/Users/sekoubafofana/projectBDS/rddSources/rdd4.txt" )

    // imposer le nbre de rartitions que je veux sur Spark
    // 1! rdd3.repartition(1).saveAsObjectFile("/Users/sekoubafofana/projectBDS/rddSources/rdd3.txt")

    // AUssi une méthode pour aggrégation = collect() = repartition dont le calcul est fait sur le Driver
    // Il permet d'obtenir un fichier.

    // 2! rdd3.collect().foreach{l => println(l)}

    // Création de RDD à partir d'un fichier source

    val rdd5 = sc.textFile("/Users/sekoubafofana/projectBDS/rddSources/Sources/rdd5.csv")
    println("Lecture du contenu du RDD 5")
    rdd5.foreach{ l => println(l)}

    println()

    // Lecture de plusieurs fichiers en une seule exécution


    val rdd6 = sc.textFile("/Users/sekoubafofana/projectBDS/rddSources/Sources/*")
    println("Lecture du contenu du RDD 6")
    rdd6.foreach{ l => println(l)}

    println()

    // Transformation RDD:

    val rdd_trans : RDD[String] = sc.parallelize(List("Alain mange une banane", "La banane est une bonne alimentation pour la santé", "Acheter de bons fruits de saison"))
    rdd_trans.foreach( l => println( "Ligne de mon RDD : " + l))

    println()

    val rdd_map = rdd_trans.map(x => x.split( " "))
    println(" Nombre d'éléments de RDD MAP : " + rdd_map.count())

    println()


    val rdd_map2 = rdd_trans.map( w => (w, w.length))
    // rdd_map2.foreach( l => println(l))

    // Une recherche par mot clé

    val rdd_map1 = rdd_trans.map( w => (w, w.length, w.contains("banane"))).map(x => (x._1.toUpperCase(), x._3, x._2))
    // rdd_map1.foreach( l => println(l))

    // Manipulation des éléments du RDD

    val rdd_map3 = rdd_map1.map(x => (x._1.toUpperCase(), x._2, x._3))
    // rdd_map3.foreach( l => println(l))

    println()

    val rdd7 = rdd_map3.map( x => (x._1.split( " "), 1))
    rdd7.foreach( l => println(l._1(0), l._2))
    // L'imcapacité de la fonction map à applatir les contenus, on fait appel à flatMap
    // flatMap applatit les éléments pour créer un nouveau RDD.

    println()

    val rdd_flat = rdd_trans.flatMap(x => x.split(" ")).map(w => (w, 1))
    rdd_flat.foreach( l => println(l))

    println()

    // Comptage des fichiers contenant dans le RDD 6

    val rdd_comptage = rdd6.flatMap(x => x.split(" ")).map(m => (m, 1)).reduceByKey((x,y) => x+y)
    rdd_comptage.repartition(1).saveAsTextFile("/Users/sekoubafofana/projectBDS/rddSources/Sources/comptageRDD.txt")
    // rdd_comptage.foreach( l => println(l))

    println()

    // transformation filter

    val rdd_filttered = rdd_flat.filter(x => (x._1.contains("banane")))
    // rdd_filttered.foreach(l => println(l))

    println()

    // Transformation reduceByKey

    val rdd_reduced = rdd_flat.reduceByKey((x,y) => x+y)
    rdd_reduced.foreach( l => println(l))

    println()

    val rdd_reduced2 : RDD[String] = sc.parallelize(List(" Lorombo est un district de la Sous-Préfecture de Cisséla et de la Préfecture de Kouroussa", "Lorombo est situé à 18 KM de Cisséla", "Lorombo est à 102 KM de Kouroussa", "Lorombo est composé de 6 quartiers", "Ils sont : Kassamala, Kalléla, Maramala, Bassila, Manignana et Doukréla .",
      "Le fondateur de Lorombo s'appelle Fodéba Kassama", "Les FOFANA de Djabiloula sont venus de Karéfamorya dans Kankan"))
    // rdd_reduced2.foreach( l => println( "Ligne de mon RDD : " + l))

    println()

    val rdd_reduced3 = rdd_reduced2.flatMap( x => x.split( " ")).map(k => (k,1))
    rdd_reduced3.foreach( l => println(l))

    println()


    val rdd_reduced4 = rdd_reduced3.reduceByKey((x,y) => x+y)
    rdd_reduced4.foreach( l => println(l))

    // Les notions de persistance des RDD :

    rdd_flat.cache() // enregistrer en mémoire
    //rdd_flat.persist(StorageLevel.MEMORY_AND_DISK)
    //rdd_flat.unpersist()

    // Notions de DataFrame : il faut d'abord importer sa biblio "implicits" qui contient dans une session Spark
    // Ici on manipule le RDD dans la DataFrame

    import session_s.implicits._ // Il faut préablament créer cette session Spark
    val df : DataFrame = rdd_flat.toDF("text", "value")
    // df.show(50)




  }


  /*
   // Première méthode de création d'une session Spark. Un déploiement en cluster par
   val ss : SparkSession = SparkSession.builder()
     .appName(name = "Mon application Saprk")
     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .config("spark.sql.crossJoin.enable", "true")
     .enableHiveSupport()
     .getOrCreate()
  */

  // 2eme Méthode, celle recommandée : je recommande de le faire avec une fonction.

  /**
   * Fonction qui initialise et instancie une session Spark
   * @param env : est une variable qui indique l'environnement dans lequel est déployée l'application
   *            Si Env = True, alors l'application est déployée en local sinon, elle est en PROD.
   */
  def sissionSpark(env : Boolean = true) : SparkSession = {
    if(env == true){
      //System.setProperty("hadoop.home.dir", "/Users/sekoubafofana/sparkStreaminIntelliJ/Hadoop")
      ss = SparkSession.builder
        .master(master = "local[*]")
        // .enableHiveSupport()
        .getOrCreate()
    } else {
      val ss : SparkSession = SparkSession.builder()
        .appName(name = "Mon application Spark")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.crossJoin.enable", "true")
        //.enableHiveSupport()
        .getOrCreate()
    }

    ss


    // RDD = Resilient Distributed Dataset ==> Spark = implémentation du RDD, set de données ou abstraction
    // Sparl :
    // 1 - Sparl SQL = module d'exécution du SQL,
    // 2- Spark Streaming = module d'exécutiopn du traitement streaming
    // 3- MLib = biblio d'algo parallélisés d'apprentissage statistiques
    // 4- GraphX = module de requête et de traitement de graphes

    // Dataset = collection, il est immutable. Distributed = Shared = partage ou partitiionnement, mais elle
    // est redondante. C-à-d chaque noeud du cluster à sa propose copie.
    // Resilient = capacité de se relever après un problème.
    // RDD = une collection de données distribuées dans les mémoires des noeuds de clusters qui se relèvent rapidement après une panne.
    // Caractéristique RDD : Lazy = il se lance quand il y a une opération, Immutable = ses collections sont non modifiables
    // et In-memory = il s'exécute en mémoire.






  }



}