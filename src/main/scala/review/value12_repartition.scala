package review

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xiaohu
 * @create 2020-10-28 1:07
 */
object value12_repartition {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.具体业务逻辑
    //3.1 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6),3)

    //3.2 重新分区
    val repartitionRDD: RDD[Int] = rdd.repartition(2)

    //3.3 打印查看分区数据
    val indexRDD: RDD[(Int, Int)] = repartitionRDD.mapPartitionsWithIndex(
      (index, items) => {
        items.map((index, _))
      }
    )

    //3.4 遍历打印输出
    indexRDD.collect().foreach(println)


    //4.关闭连接
    sc.stop()

  }
}
