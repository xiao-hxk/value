package review

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xiaohu
 * @create 2020-10-28 1:11
 */
object value13_sortBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.具体业务逻辑
    //3.1 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1,3,5,2,4,6))

    //3.2 配置为升序排
    val sortRDD1: RDD[Int] = rdd.sortBy(num=>num)
    sortRDD1.collect().foreach(println)

    println("---------------------------")

    //3.3 配置为降序排
    val sortRDD2: RDD[Int] = rdd.sortBy(num=>num,false)
    sortRDD2.collect().foreach(println)

    println("---------------------------")

    //3.4 创建一个RDD
    val strRDD: RDD[String] = sc.makeRDD(List("1","22","12","2","3"))

    //3.5 按照字符的int值来排序
    strRDD.sortBy(num=>num.toInt).collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }
}
