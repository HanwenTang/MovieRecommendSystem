/*
package com.atguigu.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String,language: String, genres: String, actors: String, directors: String )

case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String,db:String)

//标准推荐对象
case class Recommendation(mid:Int,score:Double)
//用户推荐列表，用来存储计算出来的用户可能感兴趣的电影列表的样例类
case class UserResc(uid:Int,resc:Seq[Recommendation])
//电影相似度列表样例类
case class MovieResc(mid:Int,resc:Seq[Recommendation])


object OfflineRecommender {

  //定义常量
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_Movie_COLLECTION = "Movie"
  //推荐表的名称
  val USER_RESC = "UserRecs"
  val MOVIE_RESC = "MovieRecs"
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {

    //定义spark，mongodb连接的配置信息集合
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建sparksession的上下文信息
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //创建mongodb数据库的连接
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //导入隐式转换规则
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //从mongodb中加载数据
    val ratingRDD = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map( rating=>(rating.uid,rating.mid,rating.score)).cache()

    //用户数据集RDD[Int]
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()

    //创建训练数据集
    val trainData = ratingRDD.map(x=>Rating(x._1,x._2,x._3))

    //rank是模型中隐语义因子的个数，iterations是迭代次数，lambda是ALS的正则化参数
    val (rank,iterations,lambda) = (300,5,0.01)

    //调用ALS算法训练隐语义模型
    val model = ALS.train(trainData,rank,iterations,lambda)


    //基于用户和电影的隐特征，计算预测评分，得到用户推荐列表
    //计算user和movie的笛卡尔积，得到一个空的评分矩阵
    val userMovies: RDD[(Int, Int)] = userRDD.cartesian(movieRDD)
    //调用model的predict方法预测评分
    val preRatings: RDD[Rating] = model.predict(userMovies)

    val userRecs: DataFrame = preRatings
        .filter(_.rating > 0)
        .map(rating => (rating.user,(rating.product,rating.rating)))
        .groupByKey()
        .map{
          case (uid,recs)=>UserResc(uid,recs.toList.sortWith(_._2>_._2).take(USER_MAX_RECOMMENDATION).map(x=>Recommendation(x._1,x._2)))
        }
        .toDF()

    //将用户的相似推荐列表存入mongodb数据库
    userRecs.write
        .option("uri",mongoConfig.uri)
        .option("collection",USER_RESC)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()


    //基于电影的隐特征(从model中提取电影的特征向量)，计算相似度矩阵，得到电影的相似度列表
    val movieFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map{
      case(mid,features)=>(mid,new DoubleMatrix(features))
    }

    //将所有电影做笛卡尔积，得到电影与电影之间的关系
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        // 把自己跟自己的配对过滤掉
        case (a, b) => a._1 != b._1
      }
      .map{
        case (a, b) => {
          val simScore = this.consinSim(a._2, b._2)
          ( a._1, ( b._1, simScore ) )
        }
      }
      .filter(_._2._2 > 0.6)    // 过滤出相似度大于0.6的
      .groupByKey()
      .map{
        case (mid, items) => MovieResc( mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)) )
      }
      .toDF()




    movieRecs.write
        .option("uri",mongoConfig.uri)
        .option("collection",MOVIE_RESC)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()


    //关闭资源
    spark.stop()

  }


  //计算余弦相似度（电影特征向量）
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix)={
    movie1.dot(movie2) / ( movie1.norm2() * movie2.norm2() )
  }


}

*/