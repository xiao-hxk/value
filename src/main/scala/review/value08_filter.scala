package review

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xiaohu
 * @create 2020-10-28 0:49
 */
object value08_filter {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.具体业务逻辑
    //3.1 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    //3.2 过滤出符合条件的数据
    val filterRDD: RDD[Int] = rdd.filter(_ % 2 == 0)

    //3.3 遍历打印输出
    filterRDD.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }
}
