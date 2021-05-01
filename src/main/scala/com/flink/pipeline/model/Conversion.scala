package com.flink.pipeline.model

/**
 * The data was provided in csv format and available from Kaggle. The data consists of the following variables:
 *
 * ad_id, unique ID for each ad.
 * xyz_campaign_id, an ID associated with each ad campaign of XYZ company.
 * fb_campaign_id, an ID associated with how Facebook tracks each campaign.
 * age, age of the person to whom the ad is shown.
 * gender, gender of the person to whom the add is shown
 * interest, a code specifying the category to which the persons interest belongs.
 * Impressions, the number of times the ad was shown.
 * Clicks, number of clicks on for that ad.
 * Spent, Amount paid by company xyz to Facebook, to show that ad.
 * Total conversion, Total number of people who enquired about the product after seeing the ad.
 * Approved conversion, Total number of people who bought the product after seeing the ad.
 *
 * @param ad_id
 * @param xyz_campaign_id
 * @param fb_campaign_id
 * @param age
 * @param gender
 * @param interest
 * @param impressions
 * @param clicks
 * @param spent
 * @param totalConversion
 * @param approvedConversion
 */
case class Conversion(ad_id: Long,
                      xyz_campaign_id: Long,
                      fb_campaign_id: Long,
                      age: String,
                      gender: String,
                      interest: Int,
                      impressions: Int,
                      clicks: Int,
                      spent: Double,
                      totalConversion: Int,
                      approvedConversion: Int)

object Conversion {

  def fromString(line: String): Conversion = {
    val tokens: Array[String] = line.split(",")

    if (tokens.length < 11) {
      throw new RuntimeException("Invalid record: " + line)
    }

    try {
      val ad_id = tokens(0).toLong
      val xyz_campaign_id = tokens(1).toLong
      val fb_campaign_id = tokens(2).toLong
      val age = tokens(3)
      val gender = tokens(4)
      val interest = tokens(5).toInt
      val impressions = tokens(6).toInt
      val clicks = tokens(7).toInt
      val spent = tokens(8).toDouble
      val totalConversion = tokens(9).toInt
      val approvedConversion = tokens(10).toInt

      Conversion(ad_id, xyz_campaign_id, fb_campaign_id, age, gender, interest, impressions, clicks, spent, totalConversion, approvedConversion)
    } catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid record: " + line, nfe)
    }
  }
}
