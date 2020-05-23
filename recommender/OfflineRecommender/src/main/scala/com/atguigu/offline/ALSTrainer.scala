package com.atguigu.offline

import breeze.numerics.sqrt
import com.atguigu.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object ALSTrainer {

  def main(args: Array[String]): Unit = {

    //定义spark，mongodb连接的配置信息集合
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建sparksession的上下文信息
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores2"))
    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //创建mongodb数据库的连接
    implicit val mongoConfig =  MongoConfig(config("mongo.uri"),config("mongo.db"))

    //导入隐式转换规则
    import spark.implicits._

    //从mongodb中加载评分数据
    val ratingRDD = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map( rating=> Rating(rating.uid, rating.mid, rating.score))
      .cache()

    //随机切分数据集，生成训练集和测试集合
    val splits = ratingRDD.randomSplit(Array(0.8,0.2))
    val trainingRDD = splits(0)
    val testRDD =splits(1)

    //模型参数选择，输出最优参数
    adjustALSParam(trainingRDD,testRDD)


    //关闭资源
    spark.stop()

  }

  //遍历获取最优参数的方法
  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating])={
    //对不同参数进行遍历
    val result = for(rank <- Array(50,100,200,300);lambda <- Array(0.01,0.1,1))
      yield {
        val model = ALS.train(trainData,rank,5,lambda)
        //计算当前参数对应模型的rmse，返回Double
        val rmse = getRMSE(model,testData)
        (rank,lambda,rmse)
      }
    //控制台打印输出最优参数
    println(result.minBy(_._3))
  }

  //计算RMSE（均方根误差的方法）
  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating])={

    //计算预测评分
    val userProducts: RDD[(Int, Int)] = data.map(item => (item.user,item.product))
    val predictRating: RDD[Rating] = model.predict(userProducts)

    //以uid，mid作为外键，做内连接，获取观测值与预测值，计算误差估计函数，选取最小的参数
    val observed = data.map(item=>((item.user,item.product),item.rating))
    val predict = predictRating.map(item=>((item.user,item.product),item.rating))

    //内连接得到（uid，mid）（actual，predict）
    sqrt(
      observed.join(predict).map{
        case ((uid,mid),(actual,pre))=>
          val err = actual - pre
          err*err
      }.mean()
    )

  }
}
