/*
package com.atguigu.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

//定义连接助手对象，序列化
object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("hadoop102")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://hadoop102:27017/recommender"))
}

//mongodb的配置样例类
case class MongoConfig(uri:String,db:String)

// 定义一个基准推荐对象
case class Recommendation( mid: Int, score: Double )

// 定义基于预测评分的用户推荐列表
case class UserRecs( uid: Int, recs: Seq[Recommendation] )

// 定义基于LFM电影特征向量的电影相似度列表
case class MovieRecs( mid: Int, recs: Seq[Recommendation] )


object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieResc"

  def main(args: Array[String]): Unit = {

    //配置信息的集合
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamRecommender")

    //创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //拿到streaming context
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    //导入隐式转换规则
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //从mongodb数据库中加载电影相似度矩阵数据，并广播出去
    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map {
        //转换为map，加快查询速度
        movieRecs =>
          (movieRecs.mid, movieRecs.recs.map(
            x => (x.mid, x.score)).toMap)
      }.collectAsMap()



    val simMovieMatrixBroadCast: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(simMovieMatrix)


    //定义kafka连接参数
    val kafkaParam = Map(
      "bootstrap.servers" -> "hadoop102:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    //通过kafka创建一个DStream
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
    )

    //把原始数据转换为评分流
    val ratingStream = kafkaStream.map {
      msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }


    //继续做流式处理，核心实时算法部分
    ratingStream.foreachRDD {
      rdds =>
        rdds.foreach {

          case (uid, mid, score, timestamp) => {

            println("rating data coming! >>>>>>>>>>>>")

            //1.从redis里获取当前用户最近k次评分，保存成Array[(mid, score)]
            val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

            //2.从相似度矩阵中取出与当前电影最相似的n个电影
            val candidateMovie: Array[Int] = getTopicSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

            //3.对备选电影，计算推荐优先级，得到当前用户的实时推荐列表
            val streamRecs: Array[(Int, Double)] = computeMovieScores(candidateMovie, userRecentlyRatings, simMovieMatrixBroadCast.value)

            //4.将推荐数据保存到mongodb
            saveDataToMongoDB(uid,streamRecs)
          }
        }
    }

    //开始接收和处理数据
    ssc.start()

    println(">>>>>>>>>>>>>>>> streaming started!")

    ssc.awaitTermination()

  }

  //redis操作返回的是java类，需导入相关库进行类型转换
  import scala.collection.JavaConversions._

  //1.从redis里获取当前用户最近k次评分的电影的方法，保存成Array[(mid, score)]
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis) = {
    // 从redis读取数据，用户评分数据保存在 uid:UID 为key的队列里，value是 MID:SCORE
    jedis.lrange("uid:" + uid, 0, num - 1)
      .map {
        item => // 具体每个评分又是以冒号分隔的两个值
          val attr = item.split("\\:")
          (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
      .toArray
  }

  //2.从相似度矩阵中取出与当前电影最相似的n个电影的方法
  def getTopicSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig): Array[Int] = {

    //1.从相似度矩阵中拿出所有相似的电影
    val allSimMovies = simMovies(mid).toArray

    //2.从mongodb中查询用户已看过的电影
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map {
        item => item.get("mid").toString.toInt
      }

    //3.过滤掉该用户已看过的电影，得到输出列表
    allSimMovies.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)


  }

  //3.对备选电影，计算推荐优先级，得到当前用户的实时推荐列表
  def computeMovieScores(candidateMovies: Array[Int], userRecentlyRatings: Array[(Int, Double)],
                         simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                        (implicit mongoConfig: MongoConfig): Array[(Int, Double)] = {

    //定义一个ArrayBuffer，保存每一个备选电影的基础得分
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    //定义一个hashmap，保存每一个备选电影的增强减弱因子
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    //遍历用户评分电影的相似电影（备选电影） 与 最近k次评分电影，计算备选电影的优先级别
    for (candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings) {

      //拿到备选电影与最近评分电影的相似度
      val simScore = getMoviesSimScore(candidateMovie, userRecentlyRating._1, simMovies)

      if (simScore > 0.7) {
        //计算备选电影的基础推荐得分
        scores += ((candidateMovie, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
        } else {
          decreMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
        }
      }
    }

    //根据备选电影的mid做groupby，根据公式去求最后的推荐评分
    scores.groupBy(_._1).map {
      //goupby之后得到的数据类型为：Map(mid -> ArrayBuffer[(mid,score)])
      case (mid, scoreList) => {
        (mid, scoreList.map(_._2).sum / scoreList.length +
          log(increMap.getOrDefault(mid, 1)) -
          log(decreMap.getOrDefault(mid, 1)))
      }
    }.toArray.sortWith(_._2 > _._2)
  }


  //获取两个电影间的相似度(解决可能为空的异常情况)
  def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double = {

      simMovies.get(mid1) match {
        case Some(x) => x.get(mid2) match {
          case Some(score) => score
          case None => 0.0
        }
        case None => 0.0
      }
    }

  //自定义log方法，可通过调节因子参数，控制推荐结果的颗粒度
  def log(m: Int): Double = {
      val N = 10
      math.log(m) / math.log(N)
    }

  //4.将推荐数据保存到mongodb
  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig:
      MongoConfig)  ={

    //定义到StreamRecs表的连接
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    //如果表中已有uid对应的数据，应删除
    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))

    //将streamRecs数据存入表中
    streamRecsCollection.insert(MongoDBObject("uid"->uid,
      "recs"->streamRecs.map(x=>MongoDBObject("mid"->x._1,"score"->x._2)) ))

  }


}

*/