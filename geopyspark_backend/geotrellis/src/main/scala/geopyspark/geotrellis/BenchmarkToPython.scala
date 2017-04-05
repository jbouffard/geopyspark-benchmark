package geopyspark.benchmark

import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.avro._
import geotrellis.spark.util.KryoWrapper

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.avro._

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

object BenchmarkToPython {
  def toPython[T: AvroRecordCodec](rdd: RDD[T]): (Array[Array[Byte]], String) = {
    val arr = rdd
      .map { v =>
        AvroEncoder.toBinary(v, deflate = false)
      }

    (arr.collect(), implicitly[AvroRecordCodec[T]].schema.toString)
  }

  def getHadoopRDD(
    keyType: String,
    path: String,
    sc: SparkContext): (Array[Array[Byte]], String) =
    keyType match {
      case "ProjectedExtent" =>
        toPython[(ProjectedExtent, MultibandTile)](HadoopGeoTiffRDD.spatialMultiband(path)(sc))
      case "TemporalProjectedExtent" =>
        toPython[(TemporalProjectedExtent, MultibandTile)](HadoopGeoTiffRDD.temporalMultiband(path)(sc))
    }

  def getHadoopMap(
    keyType: String,
    path: String,
    sc: SparkContext): java.util.Map[String, Any] =
    keyType match {
      case "ProjectedExtent" => {
        val (arr, schema) = toPython(HadoopGeoTiffRDD.spatialMultiband(path)(sc))

        Map("extent" -> (1.0, 2.0, 3.0, 4.0),
          "crs" -> "EPSG:3857",
          "cols" -> 515,
          "rows" -> 515,
          "noDataValue" -> "none",
          "data" -> arr,
          "schema" -> schema)
      }

      case "TemporalProjectedExtent" => {
        val (arr, schema) = toPython(HadoopGeoTiffRDD.temporalMultiband(path)(sc))

        Map("extent" -> (1.0, 2.0, 3.0, 4.0),
          "crs" -> "EPSG:3857",
          "instant" -> 1.0,
          "cols" -> 515,
          "rows" -> 515,
          "noDataValue" -> "none",
          "data" -> arr,
          "schema" -> schema)
      }
    }

  def getHadoopScalaRDD(
    path: String,
    sc: SparkContext
  ): RDD[(ProjectedExtent, MultibandTile)] =
      HadoopGeoTiffRDD.spatialMultiband(path)(sc)
}
