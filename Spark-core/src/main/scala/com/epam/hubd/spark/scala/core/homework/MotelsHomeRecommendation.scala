package com.core.hubd.spark.scala.core.homework
import com.core.hubd.spark.scala.core.homework.domain.{BidError, BidItem, EnrichedItem, Motel}
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MotelsHomeRecommendation {
  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"
  val ERROR: String = "ERROR\\_.*"
  val logger = Logger(LoggerFactory.getLogger(MotelsHomeRecommendation.getClass()))

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")
    logger.info("Start of the program!!.")
    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation"))
    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)
    sc.stop()
    logger.info("End of Program!!.")
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BidError case class
      */
    val erroneousRDD: RDD[String] = getErroneousRecords(rawBids)
    erroneousRDD.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[Motel] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  /**
    * @param sc       SparkContext
    * @param bidsPath path of bids
    * @return RDD of bids.
    *         To get the RDD which has the list of the strings
    *         in the format
    *         List(0000004, 23-25-09-2015, 1.10, 0.85, 0.52, 0.94, 1.58, 1.86, 0.48, 0.74, 2.01, 1.80, 0.82, 0.46, 0.75, 1.83, 0.77, 1.07)
    */
  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    val lines = sc.textFile(bidsPath)
    lines.map(line => line.split(",").map(_.trim).toList)

  }

  /**
    * @param rawBids RDD bids
    * @return error records RDD,
    *         TO get the collection of the all the error records in the form of the RDD
    *         02-21-09-2015,ERROR_BID_SERVICE_UNAVAILABLE,1
    *         This method will give the count of the error occured by ordering by key.
    */
  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    val errors = rawBids.filter(isErroneousRecord(_))
      .map(bid => (BidError(bid(Constants.BIDS_HEADER.indexOf("BidDate")),bid(Constants.BIDS_HEADER.indexOf("HU"))), 1))
    val counts = errors.reduceByKey((x, y) => x + y)
    counts.map(kv => kv._1 + "," + kv._2)
  }

  /**
    * @param bid list of the bid
    * @return to check that the string error is present.
    *         To check that the does the second colum of the list matches the string "ERROR"
    *         which resembles the error.
    */
  def isErroneousRecord(bid: List[String]) = bid(Constants.BIDS_HEADER.indexOf("HU")).matches(ERROR)

  /**
    * @param sc SparkContext
    * @param exchangeRatesPath
    * @return Map collection of the date and the amount.
    * To get the Map as an output which has the date as the and the price.
    *  (11-06-05-2016,0.803)
    */
  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {

    val exchnageLines = sc.textFile(exchangeRatesPath).map(_.split(Constants.DELIMITER).map(_.trim))
    val array = exchnageLines.map(line => (line(Constants.EXCHANGE_RATES_HEADER.indexOf("ValidFrom")), toDoubleWithNan(line(Constants.EXCHANGE_RATES_HEADER.indexOf("ExchangeRate"))))).collect
    array.toMap
  }

  /**
    * @param rawBids       RDD
    * @param exchangeRates RDD Map which has the date and the amount.
    * @return RDD collection of the BidItem which has the motelID, date, losa, price.
    *         in the format of 0000005,2016-02-09 07:00,US,0.883
    */
  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    //Gives the list like (0000002,11-05-08-2016,US,0.68), (0000002,11-05-08-2016,MX,1.59), (0000002,11-05-08-2016,CA,1.63)
    val bidsByLosa = rawBids.filter(!isErroneousRecord(_)).flatMap(
      bid => List((bid(Constants.BIDS_HEADER.indexOf("MotelID")), bid(Constants.BIDS_HEADER.indexOf("BidDate")), "US", bid(Constants.BIDS_HEADER.indexOf("US"))),
        (bid(Constants.BIDS_HEADER.indexOf("MotelID")), bid(Constants.BIDS_HEADER.indexOf("BidDate")), "MX", bid(Constants.BIDS_HEADER.indexOf("MX"))),
        (bid(Constants.BIDS_HEADER.indexOf("MotelID")), bid(Constants.BIDS_HEADER.indexOf("BidDate")), "CA", bid(Constants.BIDS_HEADER.indexOf("CA")))))

      //b._1 motel => motelId, b._2 => date , b._3 =>losa (ex: US, MX , CA), b._4 is the value.
    val bidsFiltered = bidsByLosa
      .map(bid => (bid._1, bid._2, bid._3,Math.round(toDoubleWithNan(bid._4) *  exchangeRates.getOrElse(bid._2, Double.NaN) * 1000) / 1000.0)).filter(!_._4.isNaN)
    bidsFiltered.map(bid => BidItem(bid._1, reformatDate(bid._2), bid._3, bid._4))
  }


  /**
    * @param sc         SparkContext
    * @param motelsPath motels path
    * @return will return the RDD of motelID and motelname.
    *         This method will implemented to get the motelID and motelname in the RDD format.
    */
  def getMotels(sc: SparkContext, motelsPath: String): RDD[Motel] = {
    val motelsMap = sc.textFile(motelsPath).map(line => line.split(Constants.DELIMITER).map(s => s.trim))
    motelsMap.map(list => Motel(list(Constants.MOTELS_HEADER.indexOf("MotelID")), list(Constants.MOTELS_HEADER.indexOf("MotelName"))))

  }


  /**
    * @param value  the string value
    * @return double value.
    *         To get the double value parsed from the string type.
    *         throws and catches the exception if the input is incompatible and cannot be parsed.
    */
  def toDoubleWithNan(value: String): Double = {
    try {
      value.toDouble
    } catch {
      case _: NumberFormatException => Double.NaN
    }
  }


  /**
    * @param bids   RDD of BidItem
    * @param motels RDD for motelID and motalname .
    *               comparing the prices and  getting the higest prices for a motel id.
    * @return RDD[EnrichedItem]
    *         This function will prodive us the set of the EnrichedItem RDD which has the
    *         motelID, motelName,BidDate, LOSA,Price. Here we will join the bids motelid and motel motelid
    *         get the according details.
    *         v1 and v2 are the prices which are used to compare the highest price bid.
    */
  def getEnriched(bids: RDD[BidItem], motels: RDD[Motel]): RDD[EnrichedItem] = {
    //
    val allEnriched = bids.map(bi => (bi.motelId, bi)).join(motels.map(m => (m.motelId, m)))

      // mapping the motelID, motelName,BidDate, LOSA,Price
      .map(bm => EnrichedItem(bm._1, bm._2._2.motelName, bm._2._1.bidDate, bm._2._1.loSa, bm._2._1.price))

    val keyed = allEnriched.map(ei => (ei.motelId, ei.bidDate) -> ei)
    keyed.reduceByKey((v1, v2) => if (comparePricesAndPlaces(v1, v2) > 0) v1 else v2).map(_._2)
  }

  /**
    * @param enrichedItem1
    * @param enrichedItem2
    * @return intrger
    *         This function is used to check the which numebr is greater when
    *         compared two numbers and then retuns the integer value.
    */
  def comparePricesAndPlaces(enrichedItem1: EnrichedItem, enrichedItem2: EnrichedItem): Int = {
    if (enrichedItem1.price > enrichedItem2.price)
      1
    else if (enrichedItem1.price < enrichedItem2.price)
      -1
    else
      Constants.TARGET_LOSAS.indexOf(enrichedItem2.loSa) - Constants.TARGET_LOSAS.indexOf(enrichedItem1.loSa)
  }

  /**
    * @param v String
    * @return reformatted date.
    *         To convert the date in the specified format as per the problem statement.
    */
  def reformatDate(v: String): String = {
    val dateTime = Constants.INPUT_DATE_FORMAT.parseDateTime(v)
    Constants.OUTPUT_DATE_FORMAT.print(dateTime)
  }
}