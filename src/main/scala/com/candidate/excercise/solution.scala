package com.candidate.excercise

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import java.util.Properties

object solution extends App {

  val spark = SparkSession
    .builder()
    .appName("Connect IBM COS")
    .master("local")
    .getOrCreate()

  //1. set spark Conf to connect to IBM COS
  connect(spark, "https://s3.us.cloud-object-storage.appdomain.cloud",
  "0aba66146f3b450cacebaa908046d17e",
  "27b804de3b329a680dbf148fd76da208f33e8a5aaaea4cbd", "myCos")

  //2. Read data from CSV File on IBM COS
  val df = readCOSFile("cos://candidate-exercise.myCos/emp-data.csv", "CSV", None, spark).cache()

  df.show(false)

  //4. Create a table  and write data based COS data schema read in Step 2
  writeDB2Table(df,"cnh46371.excercise")


  /*
  * 5. Calculate and display
  *   a.	Gender ratio in each department
  *   b.	Average salary in each department
  *   c.	Male and female salary gap in each department
   */

  genderRatio(df).show(false)
  averageSalary(df).show(false)
  salaryGap(df).show(false)

  //6. write one the calculated data as a Parquet back to COS
  writetoCOS(salaryGap(df), "Parquet", "cos://candidate-exercise.myCos/Salary-Gap.parquet")
  readCOSFile("cos://candidate-exercise.myCos/Salary-Gap", "Parquet", None, spark).show(false)

  //Below method sets spark conf to connect to IBM COS
  def connect(spark: SparkSession, endPoint: String, accessKey: String, secretKey:String, serviceName: String): Unit = {

    spark.sparkContext.hadoopConfiguration.set("fs.stocator.scheme.list", "cos")
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.service.v2.signer.type", "false")
    spark.sparkContext.hadoopConfiguration.set(s"fs.cos.${serviceName}.access.key", accessKey)
    spark.sparkContext.hadoopConfiguration.set(s"fs.cos.${serviceName}.secret.key", secretKey)
    spark.sparkContext.hadoopConfiguration.set(s"fs.cos.${serviceName}.endpoint", endPoint)
  }

  //Below method reads file from IBM COS
  def readCOSFile(filepath: String, fileType: String, schema: Option[StructType], spark: SparkSession): DataFrame = {

    schema.map(x => spark.read.format(fileType).schema(x).load(filepath))
      .getOrElse(spark.read.format(fileType).option("header", "true").load(filepath))

  }

  //Method to write data into DB2 Table
  def writeDB2Table(df:DataFrame, table: String): Unit = {
    df.write
      .format("jdbc")
      .option("driver", "com.ibm.db2.jcc.DB2Driver")
      .option("url", "jdbc:db2://dashdb-txn-sbox-yp-lon02-04.services.eu-gb.bluemix.net:50000/BLUDB")
      .option("user", "cnh46371")
      .option("password", "g1kct51@231v2zm8")
      .option("dbtable", table)
      .mode(SaveMode.Overwrite)
      .save()
  }

  //Method to read from DB2 Table
  def readDB2Table(spark: SparkSession, table: String): DataFrame = {

    val connectionProperties = new Properties()

    connectionProperties.put("user", "cnh46371")
    connectionProperties.put("password", "g1kct51@231v2zm8")
    connectionProperties.setProperty("Driver", "com.ibm.db2.jcc.DB2Driver")

    spark.read.jdbc("jdbc:db2://dashdb-txn-sbox-yp-lon02-04.services.eu-gb.bluemix.net:50000/BLUDB", table, connectionProperties)

  }

  //Method calculates Gender ratio for the input data
  def genderRatio(df: DataFrame): DataFrame = {

    df.groupBy("Gender")
      .count()
      .withColumn("Gender_Ratio", round(col("count")/sum("count").over(),3))
  }

  //Method calculates average salary for the input data
  def averageSalary(df: DataFrame): DataFrame = {

    df.withColumn("Salary", regexp_replace(col("Salary"), "\\,|\\$", ""))
      .groupBy("Department")
      .agg(round(avg("Salary"),3).alias("Avg_Salary"))
  }

  //Method calculates salary gap for the input data
  def salaryGap(df: DataFrame): DataFrame = {

    val male_df = df
      .withColumn("Salary", regexp_replace(col("Salary"), "\\,|\\$", ""))
      .filter(col("Gender") === lit("Male"))
      .withColumnRenamed("Department","M_Department")
      .groupBy("M_Department")
      .agg(round(avg("Salary"),3).alias("Avg_Male_Salary"))

    val female_df = df
      .withColumn("Salary", regexp_replace(col("Salary"), "\\,|\\$", ""))
      .filter(col("Gender") === lit("Female"))
      .groupBy("Department")
      .agg(round(avg("Salary"),3).alias("Avg_Female_Salary"))

    male_df
      .join(female_df,
        male_df.col("M_Department") === female_df.col("Department"),
        "full_outer")
      .withColumn("Salary_Gap",
        round(col("Avg_Male_Salary")-col("Avg_Female_Salary"),3))
      .select("Department", "Avg_Male_Salary", "Avg_Female_Salary", "Salary_Gap")

  }

  // Method to write data to IBM COS
  def writetoCOS(df:DataFrame, fileType: String, filePath: String): Unit = {

    df.coalesce(1)
      .write.format(fileType)
      .mode(SaveMode.Overwrite)
      .save(filePath)
  }

}
