package org.apache.spark.h2o

import org.apache.spark.SparkContext
import org.apache.spark.h2o.util.SharedSparkTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.fvec.Vec

/**
  * Testing schema for h2o schema spark dataset transformation.
  * Created by vpatryshev on 7/15/16.
  */
@RunWith(classOf[JUnitRunner])
class H2ODatasetTest  extends FunSuite with SharedSparkTestContext {
  case class SamplePerson(name: String, age: Int, email: String)
  val people:List[Product] = SamplePerson("Hermione Granger", 15, "hgranger@griffindor.edu.uk")::
               SamplePerson("Ron Weasley", 14, "rweasley@griffindor.edu.uk")::
               SamplePerson("Harry Potter", 14, "hpotter@griffindor.edu.uk")::Nil
  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)


  private type RowValueAssert = (Long, Vec) => Unit

  private def assertBasicInvariants[T<:Product](ds: Dataset[T], df: H2OFrame, rowAssert: RowValueAssert): Unit = {
    assertHolderProperties(df)
    assert (ds.count == df.numRows(), "Number of rows in H2OFrame and Dataset should match")
    // Check numbering
    val vec = df.vec(0)
    var row = 0
    while(row < df.numRows()) {
      assert (!vec.isNA(row), "The H2OFrame should not contain any NA values")
      rowAssert (row, vec)
      row += 1
    }
  }

  private def assertHolderProperties(df: H2OFrame): Unit = {
    assert (df.numCols() == 1, "H2OFrame should contain single column")
    assert (df.names().length == 1, "H2OFrame column names should have single value")
    assert (df.names()(0).equals("result"),
      "H2OFrame column name should be 'result' since Holder object was used to define Dataset")
  }
}

class PUBDEV458Type(val result: Option[Int]) extends Product with Serializable {
  //def this() = this(None)
  override def canEqual(that: Any):Boolean = that.isInstanceOf[PUBDEV458Type]
  override def productArity: Int = 1
  override def productElement(n: Int) =
    n match {
      case 0 => result
      case _ => throw new IndexOutOfBoundsException(n.toString)
    }
}
