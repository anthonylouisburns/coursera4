package timeusage

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import timeusage.TimeUsage._

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {
  //    test("take .01") {
  //      val (columns, initDf) = read("/timeusage/atussum.csv", .01)
  //      val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
  //      val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
  //      summaryDf.show()
  //      val finalDf = timeUsageGrouped(summaryDf)
  //      finalDf.show()
  //    }


  //  test("t1") {
  //    val df = rdd("/timeusage/atussum.csv")
  //    df.take(5).map(println(_))
  //
  //    val (columns, initDf) = read("/timeusage/atussum.csv", .01)
  //    initDf.show(5)
  //    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
  //
  //    val summaryDf = timeUsageSummary2(primaryNeedsColumns, workColumns, otherColumns, initDf)
  //    summaryDf.show()
  //    val finalDf = timeUsageGrouped(summaryDf)
  //    finalDf.show()
  //  }

  //  test("take .01 t1") {
  //    val df = rdd("/timeusage/atussum.csv")
  //    df.take(5).map(println(_))
  //    val (columns, initDf) = read("/timeusage/atussum.csv", .01)
  //    initDf.show(10)
  //    //    val finalDf = timeUsageGrouped(summaryDf)
  //    //    finalDf.show()
  //  }
  //
  //
  //  test("take .01 t2") {
  //    val df = rdd("/timeusage/atussum.csv")
  //    df.take(5).map(println(_))
  //    val (columns, initDf) = read("/timeusage/atussum.csv", .01)
  //
  //
  //    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
  //    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
  //    summaryDf.show()
  //  }

  test("t2") {
    val df = rdd("/timeusage/atussum.csv")
    //    df.take(5).map(println(_))

    val (columns, initDf) = read("/timeusage/atussum.csv", .01)
    //    initDf.show(5)
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)

    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf).cache()

    // timeUsageGrouped && timeUsageGroupedSql && timeUsageGroupedTyped
    val finalDf = timeUsageGrouped(summaryDf)
    val x1 = finalDf.take(20)
    val finalDf2 = timeUsageGroupedSql(summaryDf)
    val x2 = finalDf2.take(20)
    val finalDf3 = timeUsageGroupedTyped(timeUsageSummaryTyped(summaryDf))
    val x3 = finalDf3.take(20)
    println("timeUsageGrouped")
    x1.foreach(println(_))
    println("timeUsageGroupedSql")
    x2.foreach(println(_))
    println("timeUsageGroupedTyped")
    x3.foreach(println(_))
    assert(x1.sameElements(x2))
    assert(x1.map(x=>(x.get(0),x.get(1),x.get(2),x.get(3),x.get(4),x.get(5))).sameElements(x3.map(x=>(x.working,x.sex,x.age,x.primaryNeeds,x.work,x.other))))
  }
  //  +-----------+------+------+------------+-----+-----+
  //  |    working|   sex|   age|primaryNeeds| work|other|
  //  +-----------+------+------+------------+-----+-----+
  //  |not working|female| young|       723.9| 74.6|625.3|
  //    |    working|female|active|       703.5|233.7|490.3|
  //  |not working|female| elder|       585.6| 18.3|795.6|
  //    |    working|  male| elder|       618.3|269.3|544.2|
  //  |    working|female| elder|       630.4|247.5|548.9|
  //  |not working|  male|active|       658.8| 19.7|741.2|
  //    |not working|  male| young|       621.7|  0.0|816.1|
  //    |    working|  male|active|       647.0|300.8|482.5|
  //  |not working|female|active|       764.4| 11.7|657.4|
  //    |    working|female| young|       728.2|198.5|481.0|
  //  |not working|  male| elder|       576.3|170.3|693.2|
  //    |    working|  male| young|       650.3|171.1|604.1|
  //  +-----------+------+------+------------+-----+-----+
  //  timeUsageGroupedSql
  // +-----------+------+------+------------+-----+-----+
  //  |    working|   sex|   age|primaryNeeds| work|other|
  //  +-----------+------+------+------------+-----+-----+
  //  |not working|female| young|       723.9| 74.6|625.3|
  //    |    working|female|active|       703.5|233.7|490.3|
  //  |not working|female| elder|       585.6| 18.3|795.6|
  //    |    working|  male| elder|       618.3|269.3|544.2|
  //  |    working|female| elder|       630.4|247.5|548.9|
  //  |not working|  male|active|       658.8| 19.7|741.2|
  //    |not working|  male| young|       621.7|  0.0|816.1|
  //    |    working|  male|active|       647.0|300.8|482.5|
  //  |not working|female|active|       764.4| 11.7|657.4|
  //    |    working|female| young|       728.2|198.5|481.0|
  //  |not working|  male| elder|       576.3|170.3|693.2|
  //    |    working|  male| young|       650.3|171.1|604.1|
  //  +-----------+------+------+------------+-----+-----+
  //  timeUsageGroupedTyped
  // +-----------+------+------+------------+-----+-----+
  //  |    working|   sex|   age|primaryNeeds| work|other|
  //  +-----------+------+------+------------+-----+-----+
  //  |not working|female|active|       764.4| 11.7|657.4|
  //    |not working|female| elder|       585.6| 18.3|795.6|
  //    |not working|female| young|       723.9| 74.6|625.3|
  //    |not working|  male|active|       658.8| 19.7|741.2|
  //    |not working|  male| elder|       576.3|170.3|693.2|
  //    |not working|  male| young|       621.7|  0.0|816.1|
  //    |    working|female|active|       703.5|233.7|490.3|
  //  |    working|female| elder|       630.4|247.5|548.9|
  //  |    working|female| young|       728.2|198.5|481.0|
  //  |    working|  male|active|       647.0|300.8|482.5|
  //  |    working|  male| elder|       618.3|269.3|544.2|
  //  |    working|  male| young|       650.3|171.1|604.1|
  //  +-----------+------+------+------------+-----+-----+
}
