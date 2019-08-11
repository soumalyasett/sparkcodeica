package com.walmartmedia.spark.sparkexamples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType };

class ReadingTemperatureFiles(var spark: SparkSession) {

  def fun1 {

    val Rdd = spark.sparkContext
      .textFile("C:\\SampleData\\temperature_data\\stockholm_daily_temp_obs_1756_1858_t1t2t3.txt")

    val CleanedRdd = Rdd.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)

    val DfSchema = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true)))

    val NewRdd = CleanedRdd
      .map(r => r.split(",")).map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    val DF1 = spark.createDataFrame(NewRdd, DfSchema)

    DF1.show(10, false)
    print(DF1.count)

    DF1.coalesce(1).registerTempTable("DF1_table")

    spark.sql("drop table if exists default.temp_obs_1756_1858")
    spark.sql("create table default.temp_obs_1756_1858 as select * from DF1_table").show()
    spark.sql("select * from default.temp_obs_1756_1858").show(10, false)
    spark.table("default.temp_obs_1756_1858").printSchema()

  }

  def fun2 {

    val Rdd1 = spark.sparkContext
      .textFile("C:\\SampleData\\temperature_data\\stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt")

    val CleanedRdd1 =
      Rdd1.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
        .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7)))
        .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8)

    val DfSchema1 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("TempMinC", DoubleType, true),
        StructField("TempMaxC", DoubleType, true)))

    val NewRdd1 = CleanedRdd1
      .map(r => r.split(",")).map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble))

    val DF2 = spark.createDataFrame(NewRdd1, DfSchema1)

    DF2.show(10, false)
    print(DF2.count)

    DF2.coalesce(1).registerTempTable("DF2_table")

    spark.sql("drop table if exists default.temp_obs_1859_1960")
    spark.sql("create table default.temp_obs_1859_1960 as select * from DF2_table").show()
    spark.sql("select * from default.temp_obs_1859_1960").show(10, false)
    spark.table("default.temp_obs_1859_1960").printSchema()
  }

  def fun3 {

    val Rdd2 = spark.sparkContext
      .textFile("C:\\SampleData\\temperature_data\\stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt")

    val CleanedRdd2 =
      Rdd2.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
        .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
        .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)

    val DfSchema2 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("TempMinC", DoubleType, true),
        StructField("TempMaxC", DoubleType, true),
        StructField("TempMeanC", DoubleType, true)))

    val NewRdd2 = CleanedRdd2
      .map(r => r.split(",")).map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))

    val DF3 = spark.createDataFrame(NewRdd2, DfSchema2)

    DF3.show(10, false)
    print(DF3.count)

    DF3.coalesce(1).registerTempTable("DF3_table")

    spark.sql("drop table if exists default.temp_obs_1961_2012")
    spark.sql("create table default.temp_obs_1961_2012 as select * from DF3_table").show()
    spark.sql("select * from default.temp_obs_1961_2012").show(10, false)
    spark.table("default.temp_obs_1961_2012").printSchema()
  }

  def fun4 {
    val Rdd3 = spark.sparkContext
      .textFile("C:\\SampleData\\temperature_data\\stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")

    val CleanedRdd3 =
      Rdd3.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
        .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
        .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)

    val DfSchema3 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("TempMinC", DoubleType, true),
        StructField("TempMaxC", DoubleType, true),
        StructField("TempMeanC", DoubleType, true)))

    val NewRdd3 = CleanedRdd3
      .map(r => r.split(",")).map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))

    val DF4 = spark.createDataFrame(NewRdd3, DfSchema3)

    DF4.show(10, false)
    print(DF4.count)

    DF4.coalesce(1).registerTempTable("DF4_table")

    spark.sql("drop table if exists default.temp_obs_2013_2017_manual")
    spark.sql("create table default.temp_obs_2013_2017_manual as select * from DF4_table").show()
    spark.sql("select * from default.temp_obs_2013_2017_manual").show(10, false)
    spark.table("default.temp_obs_2013_2017_manual").printSchema()

  }

  def fun5 {

    val Rdd4 = spark.sparkContext
      .textFile("C:\\SampleData\\temperature_data\\stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")

    val CleanedRdd4 =
      Rdd4.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
        .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
        .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)

    val DfSchema4 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("TempMinC", DoubleType, true),
        StructField("TempMaxC", DoubleType, true),
        StructField("TempMeanC", DoubleType, true)))

    val NewRdd4 = CleanedRdd4
      .map(r => r.split(",")).map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))

    val DF5 = spark.createDataFrame(NewRdd4, DfSchema4)

    DF5.show(10, false)
    print(DF5.count)

    DF5.coalesce(1).registerTempTable("DF5_table")

    spark.sql("drop table if exists default.temp_obs_2013_2017_auto")
    spark.sql("create table default.temp_obs_2013_2017_auto as select * from DF5_table").show()
    spark.sql("select * from default.temp_obs_2013_2017_auto").show(10, false)
    spark.table("default.temp_obs_2013_2017_auto").printSchema()

  }
}