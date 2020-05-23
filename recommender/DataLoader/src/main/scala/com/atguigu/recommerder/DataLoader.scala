package com.atguigu.recommerder

//数据加载
import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

//movie数据集样例
case class Movie(mid:Int,name:String,descri:String,timelong:String,issue:String,
                 shoot:String,language:String,genres:String,actors:String,
                 directors:String)

//rating评分数据集样例
case class Rating(uid:Int,mid:Int,score:Double,timestamp:Int)

//tag电影标签数据集样例
case class Tag(uid:Int,mid:Int,tag:String,timestamp:Int)

//定义mongodb与es样例类
case class MongoConfig(uri:String,db:String)

/**
  * @param httpHosts      http主机列表，逗号分隔
  * @param transportHosts transport主机列表
  * @param index          需要操作的索引
  * @param clustername    集群名称，默认elasticsearch
  */
case class ESConfig(httpHosts:String,transportHosts:String,index:String,clustername:String)

object DataLoader {

  //定义常量
  val MOVIE_DATA_PATH = "D:\\ideaProject\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\ideaProject\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\ideaProject\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"



  def main(args: Array[String]): Unit = {

    //定义要用到的配置参数列表
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "hadoop102:9200",
      "es.transportHosts" -> "hadoop102:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )



    //创建sparkconf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    //创建sparksession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //导入隐式转换规则
    import spark.implicits._

    //加载数据
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    //将movieRDD转换为DataFram
    val movieDF = movieRDD.map(item=>{
      val attr = item.split("\\^")
      Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,attr(5).trim,attr(6).trim,
        attr(7).trim,attr(8).trim,attr(9).trim)
    }).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()



    //数据预处理
    //声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)

    //将数据保存到MongoDB
    storeDataInMongoDB(movieDF,ratingDF,tagDF)



    //导入sparkSQL函数库
    import org.apache.spark.sql.functions._

    //将Tag标签数据进行处理，把相同mid的标签合并
    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|",collect_set($"tag"))
        .as("tags"))
      .select("mid","tags")


    //将需要处理的tag数据与movie做联合，产生新的movie数据
    val movieWithTagsDF: DataFrame = movieDF.join(newTag,Seq("mid","mid"),"left")

    //声明一个ES配置的隐式参数
    implicit val eSConfig = ESConfig(config("es.httpHosts"),
      config("es.transportHosts"),config("es.index"),
      config("es.cluster.name") )

    //将数据保存到ES
//    storeDataInES(movieWithTagsDF)


    //关闭资源
    spark.stop()



  }

  //将数据写入MongoDB数据库的方法
  def storeDataInMongoDB(movieDF:DataFrame,ratingDF:DataFrame,tagDF:DataFrame)
                        (implicit mongoConfig: MongoConfig)={
    //新建一个MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //如果MongoDB中有对应的数据库，那么应该删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //将当前数据写入到MongoDB
    movieDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    tagDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    //关闭MongoDB的连接
    mongoClient.close()
  }




  def storeDataInES(movieDF:DataFrame)(implicit eSConfig: ESConfig)={

    //创建ES的配置信息
    val settings: Settings = Settings.builder().put("cluster.name",eSConfig.clustername).build()

    //创建ES客户端
    val esClient = new PreBuiltTransportClient(settings)

    //将数据添加到ES
    //1.建立索引
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String,port:String)=>{
        esClient.addTransportAddress(new
          InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }
    }

    //2.清除ES中的遗留数据
    if(esClient.admin().indices().exists(new
      IndicesExistsRequest(eSConfig.index)).actionGet().isExists){
          esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))
    }

    //3.写入数据
    movieDF.write
      .option("es.nodes",eSConfig.httpHosts)
      .option("es.http.timeout","100m")
      .option("es.mapping.id","mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index+"/"+ES_MOVIE_INDEX)

  }



}
