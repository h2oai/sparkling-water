package org.apache.spark.h2o.converters

import org.apache.spark.TaskContext
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OContext, _}
import water.Key

import scala.collection.immutable
import scala.reflect.runtime.universe._

case class MetaInfo(names:Array[String], vecTypes: Array[Byte])  {
  require(names.length > 0, "Empty meta info does not make sense")
  require(names.length == vecTypes.length, s"Different lengths: ${names.length} names, ${vecTypes.length} types")
}

case class H2OFrameFromRDDProductBuilder(hc: H2OContext, rdd: RDD[Product], frameKeyName: Option[String]) extends ConverterUtils {


  private[this] val defaultFieldNames = (i: Int) => "f" + i


  def withDefaultFieldNames() = {
    withFieldNames(defaultFieldNames)
  }

  def withFieldNames(fieldNames: Int => String): H2OFrame = {
    if(rdd.isEmpty()){
      // transform empty Seq in order to create empty H2OFrame
      hc.asH2OFrame(hc.sparkContext.parallelize(Seq.empty[Int]), frameKeyName)
    }else {
      val meta = metaInfo(fieldNames)
      withMeta(meta)
    }
  }

  def withFields(fields: List[(String, Type)]): H2OFrame = {
    if(rdd.isEmpty()){
      // transform empty Seq in order to create empty H2OFrame
      hc.asH2OFrame(hc.sparkContext.parallelize(Seq.empty[Int]), frameKeyName)
    }else{
      val meta = metaInfo(fields)
      withMeta(meta)
    }
  }

  private[this] def keyName(rdd: RDD[_], frameKeyName: Option[String]) = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand())

  private[this] def withMeta(meta: MetaInfo): H2OFrame = {
    val kn: String = keyName(rdd, frameKeyName)
    convert[Product](hc, rdd, kn, meta.names, meta.vecTypes, H2OFrameFromRDDProductBuilder.perTypedDataPartition())
  }

  import org.apache.spark.h2o.utils.H2OTypeUtils._

  def metaInfo(fieldNames: Int => String): MetaInfo = {
    //TODO: what if there is no first element
    val first = rdd.first()
    val fnames: Array[String] = (0 until first.productArity map fieldNames).toArray[String]

    val ftypes = first.productIterator map H2OFrameFromRDDProductBuilder.inferFieldType

    // Collect H2O vector types for all input types
    val vecTypes:Array[Byte] = ftypes map dataTypeToVecType toArray

    MetaInfo(fnames, vecTypes)
  }

  def metaInfo(tuples: List[(String, Type)]): MetaInfo = {
    val names = tuples map (_._1) toArray
    val vecTypes = tuples map (nt => dataTypeToVecType(nt._2)) toArray

    MetaInfo(names, vecTypes)
  }

}

object H2OFrameFromRDDProductBuilder{

  /**
    * Infers the type from Any, used for determining the types in Product RDD
    *
    * @param value the value the type of which we are trying to check, via reflection
    * @return
    */
  private[converters] def inferFieldType(value : Any): Class[_] ={
    value match {
      case n: Byte  => classOf[java.lang.Byte]
      case n: Short => classOf[java.lang.Short]
      case n: Int => classOf[java.lang.Integer]
      case n: Long => classOf[java.lang.Long]
      case n: Float => classOf[java.lang.Float]
      case n: Double => classOf[java.lang.Double]
      case n: Boolean => classOf[java.lang.Boolean]
      case n: String => classOf[java.lang.String]
      case n: java.sql.Timestamp => classOf[java.sql.Timestamp]
      case q => throw new IllegalArgumentException(s"Do not understand type $q")
    }
  }


  /**
    *
    * @param keyName key of the frame
    * @param vecTypes h2o vec types
    * @param uploadPlan plan which assigns each partition h2o node where the data from that partition will be uploaded
    * @param context spark task context
    * @param it iterator over data in the partition
    * @tparam T type of data inside the RDD
    * @return pair (partition ID, number of rows in this partition)
    */
  private[converters] def perTypedDataPartition[T<:Product]()
                                                           (keyName: String, vecTypes: Array[Byte], uploadPlan: Option[immutable.Map[Int, NodeDesc]])
                                                           (context: TaskContext, it: Iterator[T]): (Int, Long) = {
    // An array of H2O NewChunks; A place to record all the data in this partition
    val con = ConverterUtils.getWriteConverterContext(uploadPlan, context.partitionId())
    con.createChunks(keyName,vecTypes,context.partitionId())

    it.foreach(prod => { // For all rows which are subtype of Product
      for( i <- 0 until prod.productArity ) { // For all fields...
      val fld = prod.productElement(i)
        val x = fld match {
          case Some(n) => n
          case _ => fld
        }
        x match {
          case n: Number  => con.put(i, n.doubleValue())
          case n: Boolean => con.put(i, if (n) 1 else 0)
          case n: String  => con.put(i, n)
          case n : java.sql.Timestamp => con.put(i, n)
          case _ => con.putNA(i)
        }
      }
    })
    //Compress & write data in partitions to H2O Chunks
    con.closeChunks()

    // Return Partition number and number of rows in this partition
    (context.partitionId, con.numOfRows)
  }
}