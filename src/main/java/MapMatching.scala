import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import basic.{DataSourceUtil, MapUtil}
import com.vividsolutions.jts.geom.Coordinate
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by caojiaqing on 25/04/2017.
  */
object MapMatching {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("Siemens MapMatching")
    conf.setMaster("local[*]")

    //  conf.set("spark.executor.instances", "150")
    //  conf.set("spark.executor.cores", "1")
    //  conf.set("spark.executor.memory", "2G")
    //  conf.set("spark.driver.memory", "8G")
    //  conf.set("spark.driver.maxResultSize", "15G")
    //  conf.set("spark.storage.blockManagerSlaveTimeoutMs", "200000")
    //  conf.set("spark.network.timeout", "300")
    //
    //  //推测执行
    //  conf.set("spark.speculation.interval", "100")
    //  conf.set("spark.speculation.quantile", "0.75")
    //  conf.set("spark.speculation.multiplier", "1.25")
    //  conf.set("spark.speculation", "true")
    //  conf.set("spark.shuffle.consolidateFiles", "true")
    val sc = new SparkContext(conf)

    //processTaxi()
    processBus(sc,args(0),args(1).toDouble,args(2).toInt)
    //processBus(sc,"D:\\MO\\spark\\20170517",1.5,100)


  }


  def processBus(sc:SparkContext,gpsFilename:String,coe:Double,size:Int): Unit ={
    val sqlContext=new SQLContext(sc)
    val prop = new Properties()
    val properties = new Properties
    properties.load(getClass.getClassLoader.getResourceAsStream("application.properties"))
    //mysql://www.iteblog.com:3306/iteblog?user=iteblog&password=iteblog
    val url = properties.getProperty("jdbc.localSolution.url")+"?user="+properties.getProperty("jdbc.localSolution.username")+"&"+"password="+properties.getProperty("jdbc.localSolution.password")
    val busStopsRDD = sqlContext.read.jdbc(url,"busstopinfo",prop)
    val busInfo = sqlContext.read.jdbc(url,"businfo",prop)

    val busLineRDD = busStopsRDD.rdd.map(x=>x.toString()).cache()

    val busLineDirectionInfo = BusLineInfo.getBusLineDirectionInfo(busLineRDD)//RDD[(线路号,（(起点站lng,起点站lat),(终点站lng,终点站lat),上下行标志)]
    val busDirectionMap = sc.broadcast(busLineDirectionInfo.collectAsMap())


    val busMobileRDD = BusLineInfo.getBusMoblieInfo(busInfo.rdd.map(x=>x.toString()))//(手机号，线路号)

    val busMoblieMap = sc.broadcast(busMobileRDD.collectAsMap())



    sc.stop()

  }



  private def rad(d: Double): Double={
    d*math.Pi/180.0
  }



}
