package review

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xiaohu
 * @create 2020-10-28 0:38
 */
object value07_groupBy_wordcount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.具体业务逻辑
    //3.1 创建一个RDD
    val strList:List[String] = List("Hello Scala","Hello Spark","Hello World")
    val rdd: RDD[String] = sc.makeRDD(strList)

    println("--------------- 方式一 -----------------")
    //3.2 将字符串拆分为一个一个的单词
    val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))

    //3.3 将单词结果进行转换
    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map(word=>(word,1))

    //3.4 将转换后的数据分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = wordToOneRDD.groupBy(t=>t._1)

    //3.5 将分组后的数据进行结构的转换
    val wordToSumRDD: RDD[(String, Int)] = groupRDD.map {
      case (word, list) =>
        (word, list.size)
    }

    //3.6 遍历打印输出
    wordToSumRDD.collect().foreach(println)

    println("--------------- 方式二 -----------------")
    rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)


    //4.关闭连接
    sc.stop()

  }
}
