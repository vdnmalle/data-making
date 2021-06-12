import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object datamaing extends App {

  println("real time data pipeline has started")

  val spark = SparkSession.builder().appName("datamaking1")
    .master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("error")

  val kafka_bootstrap_servers =  "192.168.56.102:9092"
  val kafka_topic_name =  "transmessage"

val transaction_detail_df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers",kafka_bootstrap_servers)
  .option("subscribe",kafka_topic_name)
  .option("startingOffsets","latest")
  .load()

  val trans_detail_write_stream =  transaction_detail_df
    .writeStream
    .format("console")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .outputMode("update")
    .start()

  trans_detail_write_stream.awaitTermination()

  println("completed")


}
