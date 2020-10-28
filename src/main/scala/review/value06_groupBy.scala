package review

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xiaohu
 * @create 2020-10-28 0:33
 */
object value06_groupBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.具体业务逻辑
    //3.1 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    //3.2 按照对2取模的值来进行分组
    rdd.groupBy(_ % 2).collect().foreach(println)

    println("------------------------------")

    //3.3 创建一个RDD
    val rdd1: RDD[String] = sc.makeRDD(List("hello", "hive", "hadoop", "spark", "scala"))

    //3.4 按照首字母第一个单词相同分组
    rdd1.groupBy(str => str.substring(0, 1)).collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }
}
