package com.walmartmedia.spark.sparkexamples

import org.apache.spark.sql.SparkSession

object DriverProgram {


  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local")
      .appName("Atmosphere Temperature Findings")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    //import spark.implicits._

    print("start process")

    val RTF = new ReadingTemperatureFiles(spark)
      RTF.fun1
      RTF.fun2
      RTF.fun3
      RTF.fun4
      RTF.fun5

    val RPF = new ReadingPressureFiles(spark)

     RPF.fun1
     RPF.fun2
     RPF.fun3
     RPF.fun4
     RPF.fun5
     RPF.fun6
     RPF.fun7

    print("end process")

    spark.stop()

  }

}