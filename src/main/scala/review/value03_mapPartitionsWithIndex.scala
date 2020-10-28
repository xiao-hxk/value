package review

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xiaohu
 * @create 2020-10-27 23:42
 */
object value03_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.具体业务逻辑
    //3.1 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 4,2)

    //3.2 使得每个元素跟所在的分区号形成一个元组，组成一个新的RDD
    val indexRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index,items)=>(items.map((index,_))))

    //3.3 遍历打印
    indexRDD.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }
}
