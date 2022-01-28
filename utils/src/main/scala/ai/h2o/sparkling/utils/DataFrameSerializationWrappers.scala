package ai.h2o.sparkling.utils

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.DataFrame

object DataFrameSerializationWrappers {
  class DataFrameSerializationWrapper(private var dataFrame: DataFrame) extends Serializable {
    private val serialVersionUID = 42424201L

    def getDataFrame(): DataFrame = dataFrame

    private def readObject(inputStream: ObjectInputStream): Unit = {
      val json = inputStream.readUTF()
      dataFrame = DataFrameJsonSerialization.decodeDataFrame(json)
    }

    private def writeObject(objectOutputStream: ObjectOutputStream): Unit = {
      val json = DataFrameJsonSerialization.encodeDataFrame(dataFrame, DataFrameSerializer.default)
      objectOutputStream.writeUTF(json)
    }
  }

  class DataFrameArraySerializationWrapper(private var dataFrames: Array[DataFrame]) extends Serializable {
    private val serialVersionUID = 42424301L

    def getDataFrames(): Array[DataFrame] = dataFrames

    private def readObject(inputStream: ObjectInputStream): Unit = {
      val json = inputStream.readUTF()
      dataFrames = DataFrameJsonSerialization.decodeDataFrames(json)
    }

    private def writeObject(objectOutputStream: ObjectOutputStream): Unit = {
      val json = DataFrameJsonSerialization.encodeDataFrames(dataFrames, DataFrameSerializer.default)
      objectOutputStream.writeUTF(json)
    }
  }

  implicit def toWrapper(dataFrame: DataFrame): DataFrameSerializationWrapper = {
    if (dataFrame == null) {
      null
    } else {
      new DataFrameSerializationWrapper(dataFrame)
    }
  }

  implicit def toWrapper(dataFrames: Array[DataFrame]): DataFrameArraySerializationWrapper = {
    if (dataFrames == null) {
      null
    } else {
      new DataFrameArraySerializationWrapper(dataFrames)
    }
  }

  implicit def toDataFrame(wrapper: DataFrameSerializationWrapper): DataFrame = {
    if (wrapper == null) {
      null
    } else {
      wrapper.getDataFrame()
    }
  }

  implicit def toDataFrame(wrapper: DataFrameArraySerializationWrapper): Array[DataFrame] = {
    if (wrapper == null) {
      null
    } else {
      wrapper.getDataFrames()
    }
  }
}
