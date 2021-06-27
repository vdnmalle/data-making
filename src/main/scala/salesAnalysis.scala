import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, CharType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.Seconds
import org.apache.spark.util.random
import salesAnalysis.{displayValuedata, readFromKakfka}

object salesAnalysis {

  val KAFKA_TOPIC_NAME_CONS = "transmessage"
  val kafka_bootstrap_servers = "192.168.56.102:9092"

  // Cassandra Cluster Details
  val cassandra_connection_host = "192.168.56.102"
  val cassandra_connection_port = "9042"
  val cassandra_keyspace_name = "trans_ks"
  val cassandra_table_name = "trans_message_detail_tbl"

  //  This is spark Session creation

  val spark = SparkSession.builder().appName("real time data pipe line")
    .master("local[*]").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("error")

//This is to read the data from kafka producer on which data is produced by nifi
  def readFromKakfka(): DataFrame = {

    val readStream = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "latest")
      .load()

    readStream
  }



 // This is complete Schema for the the order details

  val transaction_detail_Schema = StructType(Array(

    StructField("results", ArrayType(StructType(Array(

      StructField("user", StructType(Array(
        StructField("gender", StringType),
        StructField("name", StructType(Array(
          StructField("title", StringType),
          StructField("first", StringType),
          StructField("last", StringType)

      ))),

      StructField("location", StructType(Array(
        StructField("street", StringType),
        StructField("city", StringType),
        StructField("state", StringType),
        StructField("zip", IntegerType)
      ))),
      StructField("email", StringType),
      StructField("username", StringType),
      StructField("password", StringType),
      StructField("salt", StringType),
      StructField("md5", StringType),
      StructField("sha1", StringType),
      StructField("sha256", StringType),
      StructField("registered", IntegerType),
      StructField("dob", IntegerType),
      StructField("phone", StringType),
      StructField("cell", StringType),
      StructField("PPS", StringType),
      StructField("picture", StructType(Array(
        StructField("large", StringType),
        StructField("medium", StringType),
        StructField("thumbnail", StringType)
      )))

      )))
    )))),


    StructField("nationality", StringType),
    StructField("seed", StringType),
    StructField("version", StringType),
    StructField("tran_detail", StructType(Array(
      StructField("tran_card_type", ArrayType(StringType)),
      StructField("product_id", StringType),
      StructField("tran_amount", DoubleType)
    )))

  ))


  def displayValuedata(): DataFrame = {

    val transaction_deatil_data = readFromKakfka().selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), transaction_detail_Schema).as("order_details"))
      .select("order_details.*")
    transaction_deatil_data
  }

  def displayonconsole() = {

    val withoutuserdetail =   displayValuedata().select(explode(col("results.user")).alias("user"),
      col("nationality"),
      col("seed"),
      col("version"),
      col("tran_detail.tran_card_type").alias("tran_card_type"),
      col("tran_detail.product_id").alias("product_id"),
      col("tran_detail.tran_amount").alias("tran_amount")
    )

withoutuserdetail
  }

  def withuserdeatail = {

   val with_user_details_df =  displayonconsole().select(
      col("user.gender"),
      col("user.name.title"),
      col("user.name.first"),
      col("user.name.last"),
      col("user.location.street"),
      col("user.location.city"),
      col("user.location.state"),
      col("user.location.zip"),
      col("user.email"),
      col("user.username"),
      col("user.password"),
      col("user.salt"),
      col("user.md5"),
      col("user.sha1"),
      col("user.sha256"),
      col("user.registered"),
      col("user.dob"),
      col("user.phone"),
      col("user.cell"),
      col("user.PPS"),
      col("user.picture.large"),
      col("user.picture.medium"),
      col("user.picture.thumbnail"),
      col("nationality"),
      col("seed"),
      col("version"),
      col("tran_card_type"),
      col("product_id"),
      col("tran_amount")
    )
    with_user_details_df
  }



  val with_userdetail_final  = withuserdeatail.select(
    col("gender"),
    col("title"),
    col("first").alias("first_name"),
    col("last").alias("last_name"),
    col("street"),
    col("city"),
    col("state"),
    col("zip"),
    col("email"),
    concat(col("username"), round(rand() * 1000, 0).cast(IntegerType)).alias("user_id"),
    col("password"),
    col("salt"),
    col("md5"),
    col("sha1"),
    col("sha256"),
    col("registered"),
    col("dob"),
    col("phone"),
    col("cell"),
    col("PPS"),
    col("large"),
    col("medium"),
    col("thumbnail"),
    col("nationality"),
    col("seed"),
    col("version"),
    (col("tran_card_type")),
    concat(col("product_id"), round(rand() * 100, 0).cast(IntegerType)).alias("product_id"),
    round(rand() * col("tran_amount"), 2).alias("tran_amount")
  )


  val with_unix_timestamp = with_userdetail_final.withColumn("tran_date",
    from_unixtime(col("registered"), "yyyy-MM-dd HH:mm:ss"))





  def main(args: Array[String]): Unit = {

    println("kafka reading is completed")
    readFromKakfka().printSchema()
    withuserdeatail.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }
}