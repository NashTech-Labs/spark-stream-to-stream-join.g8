package com.knoldus.model

import java.sql.Timestamp


case class ImageDetails(cameraId: String, imageId: String, imageUrl: String, timeStamp: Timestamp) {
  override def toString: String = s"""{"cameraId":"$cameraId", "imageId":"$imageId", "imageUrl":"$imageUrl", "timestamp":"$timeStamp"}"""
}


case class GpsDetails(gpscameraId: String, gpsId: String, lat: Double, lon: Double, gpsTimeStamp: Timestamp) {
  override def toString: String = s"""{"gpscameraId":"$gpscameraId", "gpsId":"$gpsId", "lat":$lat, "lon":$lon, "gpsTimestamp":"$gpsTimeStamp"}"""
}
