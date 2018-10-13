package com.core.hubd.spark.scala.core.homework

import java.io.File
import java.nio.file.Files

import MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.core.hubd.spark.scala.core.homework.domain.{BidItem, EnrichedItem, Motel}
import com.core.hubd.spark.scala.core.util.RddComparator
import MotelsHomeRecommendation.ERRONEOUS_DIR
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, SparkContextProvider}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

/**
  * Created by Csaba_Bejan on 8/17/2016.
  */
class MotelsHomeRecommendationTest extends FunSuite with SharedSparkContext with RDDComparisons with BeforeAndAfter
  with BeforeAndAfterAll with SparkContextProvider {

  override def conf = new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test")

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"
  val INPUT_RATES_SAMPLE = "src/test/resources/exchange_rates_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.txt"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.txt"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/error_records"

  private var outputFolder: File = null

  before {
    outputFolder = Files.createTempDirectory("output").toFile
  }

  test("should read raw bids") {
    val expected = sc.parallelize(
      Seq(
        List("0000002", "15-04-08-2016", "0.92", "1.68", "0.81", "0.68", "1.59", "", "1.63", "1.77", "2.06", "0.66", "1.53", "", "0.32", "0.88", "0.83", "1.01"),
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL")
      )
    )

    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)

    assertRDDEquals(expected, rawBids)
  }

  test("should collect erroneous records") {
    val rawBids = sc.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0.89"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("4", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    )

    val expected = sc.parallelize(
      Seq(
        "06-05-02-2016,ERROR_1,2",
        "06-05-02-2016,ERROR_2,1",
        "07-05-02-2016,ERROR_2,1"
      )
    )

    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids)

    assertRDDEquals(expected, erroneousRecords)
  }

  test("should get exchange rates") {
    val expected = Map(
      "15-04-08-2016" -> 0.803,
      "11-05-08-2016" -> 0.873,
      "10-06-11-2015" -> 0.987
    )

    val rates = MotelsHomeRecommendation.getExchangeRates(sc, INPUT_RATES_SAMPLE)

    assert(expected, rates)
  }

  test("should get bid items") {
    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)
    val rates = MotelsHomeRecommendation.getExchangeRates(sc, INPUT_RATES_SAMPLE)

    val expected = sc.parallelize(Seq(
      BidItem(
        motelId = "0000002",
        bidDate = "2016-08-04 15:00",
        loSa = "US",
        price = 0.546
      ),
      BidItem(
        motelId = "0000002",
        bidDate = "2016-08-04 15:00",
        loSa = "MX",
        price = 1.277
      ),
      BidItem(
        motelId = "0000002",
        bidDate = "2016-08-04 15:00",
        loSa = "CA",
        price = 1.309
      )
    ))

    val bids = MotelsHomeRecommendation.getBids(rawBids, rates)

    assertRDDEquals(expected, bids)
  }

  test("should read motels") {
    val expected = sc.parallelize(Seq(Motel(
      "0000001", "Olinda Windsor Inn"),
      Motel("0000002", "Merlin Por Motel"),
      Motel("0000003", "Olinda Big River Casino"),
      Motel("0000004", "Majestic Big River Elegance Plaza"),
      Motel("0000005", "Majestic Ibiza Por Hostel"),
      Motel("0000006", "Mengo Elegance River Side Hotel"),
      Motel("0000007", "Big River Copacabana Inn"),
      Motel("0000008", "Sheraton Moos' Motor Inn"),
      Motel("0000009", "Moon Light Sun Sine Inn"),
      Motel("0000010", "Copacabana Motor Inn")
    ))

    val motels = MotelsHomeRecommendation.getMotels(sc, INPUT_MOTELS_INTEGRATION)

    assertRDDEquals(expected, motels)
  }

  test("should filter errors and create correct aggregates") {

    runIntegrationTest()



    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertAggregatedFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  after {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sc, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    assertRDDEquals(expected, actual)
  }

  private def assertAggregatedFiles(expectedPath: String, actualPath: String) = {
    val expected = parseLastDouble(sc.textFile(expectedPath)).collect.toMap
    val actual = parseLastDouble(sc.textFile(actualPath)).collect.toMap
    if (expected.size != actual.size) {
      Assert.fail(s"Aggregated have wrong number of records (${actual.size} instead of ${expected.size})")
    }
    expected.foreach(x => {
      val key = x._1
      val expectedValue = x._2
      if (!actual.contains(key)) {
        Assert.fail(s"Aggregated does not contain: $key,$expectedValue")
      }
      val actualValue = actual(key)
      if (Math.abs(expectedValue - actualValue) > 0.0011) {
        Assert.fail(s"Aggregated have different value for: $key ($actualValue instead of $expectedValue)")
      }
    })
  }
  
  private def parseLastDouble(rdd: RDD[String]) = {
    rdd.map(s => {
      val commaIndex = s.lastIndexOf(",")
      (s.substring(0, commaIndex), s.substring(commaIndex + 1).toDouble)
    })
  }

  private def printRddDifferences(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}
