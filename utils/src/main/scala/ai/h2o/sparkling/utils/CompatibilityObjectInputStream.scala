package ai.h2o.sparkling.utils

import java.io.{InputStream, ObjectInputStream, ObjectStreamClass}

import org.apache.spark.expose.Logging

import scala.collection.mutable

/**
  * The input stream ignores serialVersionUID
  */
class CompatibilityObjectInputStream(input: InputStream) extends ObjectInputStream(input) with Logging {

  protected override def readClassDescriptor(): ObjectStreamClass = {
    val streamClassDescriptor = super.readClassDescriptor()
    val localClass = Class.forName(streamClassDescriptor.getName)
    val localClassDescriptor = ObjectStreamClass.lookup(localClass)
    val localSUID = localClassDescriptor.getSerialVersionUID
    val streamSUID = streamClassDescriptor.getSerialVersionUID
    if (streamSUID != localSUID) {
      logWarning(
        s"A serialVersionUID mismatch (local=$localSUID, stream=$streamSUID) when " +
          s"deserializing ${streamClassDescriptor.getName}.")
    }
    localClassDescriptor
  }
}
