package com.knoldus.api

import java.sql.Timestamp

import com.knoldus.model.{GpsDetails, ImageDetails}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class StreamToStreamJoin(spark: SparkSession) {

  def aggregateOnWindow(imageStream: Dataset[ImageDetails], gpsDetails: Dataset[GpsDetails], win: Long) = {
    spark.udf.register("time_in_milliseconds", (str: String) => Timestamp.valueOf(str).getTime)
    imageStream.withWatermark("timestamp", "1 seconds").join(
      gpsDetails.withWatermark("gpsTimestamp", "1 seconds"),
      expr(
        s"""
            cameraId = gpscameraId AND
            abs(time_in_milliseconds(timestamp) - time_in_milliseconds(gpsTimestamp)) <= $win
         """.stripMargin)
    ).selectExpr("ImageId", "timestamp", "gpsTimestamp", "abs(time_in_milliseconds(gpsTimestamp) - time_in_milliseconds(timestamp)) as diff")
      /*.withWatermark("timestamp", "1 seconds")*/
      .groupBy("ImageId", "timestamp")
      .agg(min("diff")).withColumnRenamed("min(diff)", "nearest")

  }
}