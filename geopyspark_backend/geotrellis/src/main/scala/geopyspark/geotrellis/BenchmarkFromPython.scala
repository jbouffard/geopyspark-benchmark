package geopyspark.benchmark

import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.avro._
import geotrellis.spark.util.KryoWrapper

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.api.java.JavaRDD
import org.apache.avro._

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

object BenchmarkFromPython {
  def fromPython[T: AvroRecordCodec: ClassTag]
  (rdd: RDD[Array[Byte]], schemaJson: String): RDD[T] = {
    val schema = Some(schemaJson).map { json => (new Schema.Parser).parse(json) }
    val _recordCodec = implicitly[AvroRecordCodec[T]]
    val kwWriterSchema = KryoWrapper(schema)

    rdd.map { bytes =>
      AvroEncoder
        .fromBinary(kwWriterSchema.value.getOrElse(_recordCodec.schema), bytes, false)(_recordCodec)
    }
  }

  def toPython[T: AvroRecordCodec](rdd: RDD[T]): (JavaRDD[Array[Byte]], String) = {
    val arr = rdd
      .map { v =>
        AvroEncoder.toBinary(v, deflate = false)
      }

    (arr, implicitly[AvroRecordCodec[T]].schema.toString)
  }

  def takePythonRDD(
    javardd: JavaRDD[Array[Byte]],
    schema: String): Unit = {
      val deserialized = fromPython[(ProjectedExtent, MultibandTile)](javardd, schema)
      deserialized.first()
  }

  def takeScalaRDD(rdd: RDD[(ProjectedExtent, MultibandTile)]): Unit =
    rdd.first()

  def getHadoopRDD(
    path: String,
    sc: SparkContext
  ): (JavaRDD[Array[Byte]], String) =
      toPython(HadoopGeoTiffRDD.spatialMultiband(path)(sc))
}
