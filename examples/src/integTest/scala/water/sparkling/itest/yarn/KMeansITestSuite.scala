package water.sparkling.itest.yarn

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{SparkITest, YarnTest}

/**
 * Created by michal on 2/9/15.
 */
@RunWith(classOf[JUnitRunner])
class KMeansITestSuite extends FunSuite with SparkITest {

  test("MLlib KMeans on airlines_all data", YarnTest) {

    launch( "water.sparkling.itest.KMeansITest",
      env {
        sparkMaster("yarn-client")
      }
    )
  }
}
