package review

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xiaohu
 * @create 2020-10-27 23:34
 */
object value02_mapPartitions {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.具体业务逻辑
    //3.1 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    //3.2 调用mapPartitions方法，每个元素乘以2
    val rdd1: RDD[Int] = rdd.mapPartitions(x => x.map(_ * 2))

    //3.2 打印修改后的RDD中的数据
    rdd1.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }
}
