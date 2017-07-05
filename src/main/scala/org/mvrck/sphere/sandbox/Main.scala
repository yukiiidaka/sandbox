package org.mvrck.sphere.sandbox

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object Main {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("================================= start =================================")
    val spark = SparkSession.builder().getOrCreate()
    try {
      execute(spark)
    } finally {
      spark.stop()
    }
    logger.info("================================= end =================================")
  }

  def execute(spark: SparkSession): Unit = {
    
val df_click = spark.read.orc("hdfs:///var/data/spark/sphere-click-joined/v1/")
val df_imp = spark.read.orc("hdfs:///var/data/spark/sphere-imp-joined/")

val df_imp01 = df_imp.filter($"rtb_id".isNotNull)
val df_imp02 = df_imp01.filter($"imp_id".isNotNull)
val df_click01 = df_click.filter($"rtb_id".isNotNull)
val df_click02 = df_click01.filter($"imp_id".isNotNull)

import org.apache.spark.sql.functions.{concat, lit}
val df_imp2 =df_imp02.withColumn("IMPRTB",concat(df_imp("imp_id"), lit(""), df_imp("rtb_id")))
val df_click2 =df_click02.withColumn("IMPRTB",concat(df_click("imp_id"), lit(""), df_click("rtb_id")))

val df_imp3 =df_imp2.withColumn("dayofweek",from_unixtime(df_imp2("timestamp")/1000,"EEEEE"))
val df_click3 =df_click2.withColumn("dayofweek",from_unixtime(df_click2("timestamp")/1000,"EEEEE"))

val df_imp4 =df_imp3.withColumn("datetime",from_unixtime(df_imp3("timestamp")/1000))
val df_click4 =df_click3.withColumn("datetime",from_unixtime(df_click3("timestamp")/1000))

val df_imp5 =df_imp4.filter(($"dss_uid" !== "-----") && $"dss_uid".isNotNull)
val df_click5 =df_click4.filter(($"dss_uid" !== "-----") && $"dss_uid".isNotNull)

val df_imp6 =df_imp5.filter(df_imp5("datetime").lt(lit("2017-06-14")) && df_imp5("datetime").gt(lit("2017-06-01"))   )
val df_click6 =df_click5.filter(df_click5("datetime").lt(lit("2017-06-14")) && df_click5("datetime").gt(lit("2017-06-01"))   )


val df_imp7 =df_imp6.withColumn("imp1",lit(1))
val df_click7 =df_click6.withColumn("click1",lit(1))

val IMP_CLK =  (df_imp7.join(df_click7, df_imp7("IMPRTB") === df_click7("IMPRTB"), "left_outer").select(df_imp7("IMPRTB").alias("IMPRTB"),
df_imp7("rtb_id").alias("IMP_rtbID"), df_imp7("imp_id").alias("IMP_impID"),df_click7("rtb_id").alias("CLK_rtbID"), df_click7("imp_id").alias("CLK_impID"),
df_imp7("ad_group_id").alias("ad_group_id"),df_imp7("ad_id").alias("ad_id"),df_imp7("campaign_id").alias("campaign_id"),
df_imp7("cookie_uid").alias("cookie_uid"),df_imp7("creative_id").alias("creative_id"),df_imp7("dss_uid").alias("dss_uid"),df_imp7("ua").alias("ua"),
df_imp7("url").alias("url"), df_imp7("url_scheme").alias("url_scheme"),df_imp7("re_target").alias("re_target"),df_imp7("datetime").alias("datetime"),
df_imp7("dayofweek").alias("dayofweek"),df_imp7("imp1").alias("imp1"),df_click7("click1").alias("click1")))

val IMP_CLK1 = IMP_CLK.na.fill(0,Array("click1"))

import org.apache.spark.sql.functions.lag
import org.apache.spark.sql.expressions.Window

val IMP_CLK2=IMP_CLK1.withColumn("Clk_3_uid",sum($"click1").over(Window.partitionBy($"dss_uid").orderBy($"datetime").rowsBetween(1, 10)))
val IMP_CLK3=IMP_CLK2.withColumn("Clk_5_uid",sum($"click1").over(Window.partitionBy($"dss_uid").orderBy($"datetime").rowsBetween(1, 50)))
val IMP_CLK4=IMP_CLK3.withColumn("Clk_10_uid",sum($"click1").over(Window.partitionBy($"dss_uid").orderBy($"datetime").rowsBetween(1, 100)))
val IMP_CLK5=IMP_CLK4.withColumn("Clk_50_uid",sum($"click1").over(Window.partitionBy($"dss_uid").orderBy($"datetime").rowsBetween(1, 300)))
val IMP_CLK6=IMP_CLK5.withColumn("Clk_3_ad",sum($"click1").over(Window.partitionBy($"ad_group_id").orderBy($"datetime").rowsBetween(1, 10)))
val IMP_CLK7=IMP_CLK6.withColumn("Clk_5_ad",sum($"click1").over(Window.partitionBy($"ad_group_id").orderBy($"datetime").rowsBetween(1, 50)))
val IMP_CLK8=IMP_CLK7.withColumn("Clk_10_ad",sum($"click1").over(Window.partitionBy($"ad_group_id").orderBy($"datetime").rowsBetween(1, 100)))
val IMP_CLK9=IMP_CLK8.withColumn("Clk_50_ad",sum($"click1").over(Window.partitionBy($"ad_group_id").orderBy($"datetime").rowsBetween(1, 300)))

"""
scala> IMP_CLK10.filter($"Clk_10_uid"===0).count()
res13: Long = 107915263
scala> IMP_CLK10.filter($"Clk_300_uid"===0).count()
res14: Long = 90587426
scala> IMP_CLK10.filter($"Clk_300_uid".isNull).count()
res15: Long = 18190612
"""
val IMP_CLK10=IMP_CLK9.withColumn("HOUR",hour(IMP_CLK9("datetime")))
"""
IMP_CLK10.na.fill(0,Array("Clk_10_uid"))
IMP_CLK10.na.fill(0,Array("Clk_50_uid"))
IMP_CLK10.na.fill(0,Array("Clk_100_uid"))
IMP_CLK10.na.fill(0,Array("Clk_300_uid"))
IMP_CLK10.na.fill(0,Array("Clk_10_ad"))
IMP_CLK10.na.fill(0,Array("Clk_50_ad"))
IMP_CLK10.na.fill(0,Array("Clk_100_ad"))
IMP_CLK10.na.fill(0,Array("Clk_300_ad"))
"""
val IMP_CLK11 = IMP_CLK10.withColumn("user_past3",lag($"imp1", 3).over(Window.partitionBy($"dss_uid").orderBy($"datetime")))
val IMP_CLK12 = IMP_CLK11.withColumn("user_past5",lag($"imp1", 5).over(Window.partitionBy($"dss_uid").orderBy($"datetime")))
val IMP_CLK13 = IMP_CLK12.withColumn("user_past10",lag($"imp1", 10).over(Window.partitionBy($"dss_uid").orderBy($"datetime")))
val IMP_CLK14 = IMP_CLK13.withColumn("user_past50",lag($"imp1", 50).over(Window.partitionBy($"dss_uid").orderBy($"datetime")))
val IMP_CLK15 = IMP_CLK14.withColumn("ad_past3",lag($"imp1", 3).over(Window.partitionBy($"ad_group_id").orderBy($"datetime")))
val IMP_CLK16 = IMP_CLK15.withColumn("ad_past5",lag($"imp1", 5).over(Window.partitionBy($"ad_group_id").orderBy($"datetime")))
val IMP_CLK17 = IMP_CLK16.withColumn("ad_past10",lag($"imp1", 10).over(Window.partitionBy($"ad_group_id").orderBy($"datetime")))
val IMP_CLK18 = IMP_CLK17.withColumn("ad_past50",lag($"imp1", 50).over(Window.partitionBy($"ad_group_id").orderBy($"datetime")))

"""
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
IMP_CLK18.toDF().registerTempTable("my_table")

val IMP_CLK_with_click_flag = (sqlContext.sql("select *, case when (IMP_rtbID is not null or IMP_impID is not null)
and (CLK_rtbID is null and CLK_impID is null) then 0 when (IMP_rtbID is not null or IMP_impID is not null) and
(CLK_rtbID is not null or CLK_impID is not null) then 1 else 999 end as click_flag from my_table"))
"""
val IMP_CLK_final =IMP_CLK18.filter(IMP_CLK_with_click_flag("datetime").gt(lit("2017-06-08")))

val fraction = Map(1 -> 0.032, 0 -> 0.032)

val sampled = IMP_CLK_final.stat.sampleBy("click1",fraction,36L)
sampled.coalesce(1).write.option("header","true")csv("hdfs:///user/yuki.iidaka/20170705_sampled_retry")

    logger.info("test")
  }

}
