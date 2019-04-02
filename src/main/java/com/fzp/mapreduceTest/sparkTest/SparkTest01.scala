package com.fzp.mapreduceTest.sparkTest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkTest01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("sparkTest01").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val infos: RDD[String] = sc.textFile("infos",3)
    // 全局累加器
//    val accum = sc.accumulator(0)
    var result = infos
      .map(info => {
      val array: Array[String] = info.split(",")
      val age = array(5).toInt
      var ageTu = (if (age >= 0 && age < 35) "青年" else if (age >= 35 && age < 60) "中年" else "老年", 1)
      Array((array(2), 1), (array(3), 1), ageTu, (array(6), 1))
    })
      .flatMap(_.map(v => v ))
      .reduceByKey(_+_)
      .zipWithIndex
      .sortBy(_._2)
      .map(v => {
        v._2+1 + "," + v._1._1 + "," + v._1._2
      })
    // 第一题
    result.saveAsTextFile("flags")
    // 第二题重新读取
    val flags = sc.textFile("flags")

    val products = sc.textFile("products")

    val orders = sc.textFile("orders")

    val sparkSql = new SQLContext(sc)
    import sparkSql.implicits._
    infos.map(info => {
      val array = info.split(",")
      (array(0), array(2), array(3), array(5), array(6))
    }).toDF("id","sex","locate","age","career").registerTempTable("infos")

    flags.map(flag => {
      val array = flag.split(",")
      (array(0), array(1))
    }).toDF("id","flag").registerTempTable("flags")

    orders.map(order => {
      val array = order.split(",")
      (array(0), array(1), array(2), array(3).toInt)
    }).toDF("id","userID","pID","num").registerTempTable("orders")

    products.map(product => {
      val array = product.split(",")
      (array(0), array(1))
    }).toDF("id","product").registerTempTable("products")

    val sql = "select a.oid , flags1.id flag1 , flags2.id flag2, flags3.id flag3,flags4.id flag4, products.product , " +
      "case " +
      " when num>=0 AND num <=3 then '小份' " +
      " when num>4 AND num <=6 then '中份' " +
      " when num>7 AND num <=10 then '大份' " +
      " else '特大份' " +
      " end numStr " +
      " from (select orders.id oid , " +
      " case " +
      " when age>=0 AND age <35 then '青年' " +
      " when age>=35 AND age <60 then '中年' " +
      " else '老年' " +
      " end ageStr " +
      " ,* from orders join infos on orders.userID = infos.id) a " +
      "join flags flags1 on a.sex = flags1.flag " +
      "join flags flags2 on a.locate = flags2.flag " +
      "join flags flags3 on a.ageStr = flags3.flag " +
      "join flags flags4 on a.career = flags4.flag " +
      "join products on a.pID = products.id"

    val resultDF: DataFrame = sparkSql.sql(sql)
    resultDF.show()
    resultDF.rdd.saveAsTextFile("result")



    sc.stop()
  }
}
