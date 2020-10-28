package review

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xiaohu
 * @create 2020-10-28 0:55
 */
object value11_coalesce {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.具体业务逻辑
    //3.1 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 4)

    //3.2 执行缩减分区操作
    val coalesceRDD: RDD[Int] = rdd.coalesce(2)

    //3.3 查看对应分区数据
    val indexRDD: RDD[(Int, Int)] = coalesceRDD.mapPartitionsWithIndex(
      (index, items) => {
        items.map((index, _))
      }
    )

    //3.4 遍历打印数据
    indexRDD.collect().foreach(println)

    println("---------------------------------")

    //3.5 创建一个分区
    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6),3)

    //3.6 执行coalesce操作(默认不执行shuffle)
    val coalesceRDD1: RDD[Int] = rdd1.coalesce(2)
//    //3.6 （指定执行shuffle）
//    val coalesceRDD1: RDD[Int] = rdd1.coalesce(2,true)

    //3.7 打印分区号
    val indexRDD1: RDD[(Int, Int)] = coalesceRDD1.mapPartitionsWithIndex(
      (index, items) => {
        items.map((index, _))
      }
    )

    //3.8 遍历打印输出
    indexRDD1.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }
}
