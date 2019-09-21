package com.knoldus

import java.sql.Timestamp
import java.time.Instant

import com.knoldus.api.StreamToStreamJoin
import com.knoldus.model.{GpsDetails, ImageDetails}
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class StreamJoinSpec extends WordSpec with EmbeddedKafka {

  implicit val serializer = new StringSerializer()

  def publishImagesToKafka(start: Int, end: Int) = {
    start to end foreach { recordNum =>
      //TODO Add Serializer and dser
      val imageDetails = ImageDetails((recordNum % 4).toString, recordNum.toString, recordNum.toString, Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis())))
      publishToKafka("camerasource", imageDetails.toString)
      println(imageDetails)
      Thread.sleep(1000)
    }
  }

  def publishGPSDataToKafka(start: Int, end: Int) = {
    start to end foreach { recordNum =>
      //TODO Add Serializer and dser
      val gpsDetails = GpsDetails((recordNum % 4).toString, recordNum.toString, recordNum.toDouble, recordNum.toDouble, Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis())))
      println(gpsDetails)
      publishToKafka("gpssource", gpsDetails.toString)
      Thread.sleep(100)
    }
  }

  "StreamToStreamJoin" should {

    "aggregateOnWindow for a duration of 500 seconds" in {

      val window = 500

      val testSession =
        SparkSession
          .builder()
          .appName("StreamToStreamJoinTest")
          .master("local")
          .getOrCreate()

      val sc = testSession.sparkContext
      sc.setLogLevel("WARN")

      val sut = new StreamToStreamJoin(testSession)

      withRunningKafka {
        val imagesDf = testSession
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:6001")
          .option("subscribe", "camerasource")
          .option("startingOffsets", "earliest")
          .option("checkpointLocation", "/home/knoldus/")
          .load()

        val gpssDf = testSession
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:6001")
          .option("subscribe", "gpssource")
          .option("checkpointLocation", "/home/knoldus/")
          .load()

        implicit val imageEncoder: Encoder[ImageDetails] = Encoders.product[ImageDetails]
        implicit val gpsEncoder: Encoder[GpsDetails] = Encoders.product[GpsDetails]

        val imagesSchema = StructType(
          Seq(
            StructField("cameraId", StringType),
            StructField("imageId", StringType),
            StructField("imageUrl", StringType),
            StructField("timestamp", TimestampType)
          )
        )

        val gpsSchema = StructType(
          Seq(
            StructField("gpscameraId", StringType),
            StructField("gpsId", StringType),
            StructField("lat", DoubleType),
            StructField("lon", DoubleType),
            StructField("gpsTimestamp", TimestampType)
          )
        )

        val imageDf = imagesDf.select(from_json(column("value").cast(StringType), imagesSchema).as[ImageDetails])
        val gpsDf = gpssDf.select(from_json(column("value").cast(StringType), gpsSchema).as[GpsDetails])

        testSession.udf.register("time_in_milliseconds", (str: String) => Timestamp.valueOf(str).getTime)

        val joinedDef = new StreamToStreamJoin(testSession).aggregateOnWindow(imageDf, gpsDf, window)

        joinedDef.printSchema()


        val query =
          joinedDef.select( "ImageId", "timestamp", "nearest" )
            .writeStream
            .outputMode("Append")
            .option("truncate", "false")
            .format("console")
            /*.option("kafka.bootstrap.servers", "localhost:6001")
            .option("topic", "output")
            .option("checkpointLocation", "/home/knoldus/")*/
            .start()

        //Generate 1,000 Image data with a velocity of 1 Record / Sec
        Future {
          publishImagesToKafka(1, 1000)
        }

        //Generate 10,000 Gps Data with a velocity of 10 Records / Sec
        Future {
          publishGPSDataToKafka(1, 10000)
        }

        query.awaitTermination()

        implicit val deserializer = new serialization.StringDeserializer()
        val ret = consumeFirstMessageFrom("gpssource")

        println(s"The published topic :::::::$ret")
      }

    }

  }

}