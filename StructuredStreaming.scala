/**
  * Created by manji on 20-01-2020.
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
object StructuredStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Readfile").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val schema = StructType(List(
      StructField("Date", TimestampType, true),
      StructField("Message", StringType, true)
    ))
    val streamDF = spark.readStream.option("delimiter", "|").schema(schema).csv("C:\\Users\\manji\\Helloworld\\src\\main\\Resources\\Logs\\1.txt")
    streamDF.createOrReplaceTempView("SDF")
    val outDF = spark.sql("select * from SDF")
    outDF.writeStream.format("console").outputMode("append").start().awaitTermination()
  }
}