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
    //processBus(sc,args(0),args(1).toDouble,args(2).toInt)
    //processBus(sc,"D:\\MO\\spark\\20170517",1.5,100)
    processBus(sc,"hdfs://10.192.27.224:8020/bus_gps/20170506",1.5,100)

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

    val busDirectionMap = sc.broadcast(busLineDirectionInfo.filter{x=>x._2._3!=1}.collectAsMap().toMap)


    val busMobileRDD = BusLineInfo.getBusMoblieInfo(busInfo.rdd.map(x=>x.toString()))//(手机号，线路号)

    val busMoblieMap = sc.broadcast(busMobileRDD.collectAsMap().toMap)



    val busLineInfo = BusLineInfo.getBusLineInfo(busLineRDD,size)//RDD(线路号，Map[(起始站点hash,终点站点hash),(起始站点名，终点站点名)])

    val busLineDetailInfo = sc.broadcast(BusLineInfo.getBusLineDetailInfo(busLineRDD).collectAsMap().toMap)

    //Map[(线路号，List[站点])]
    val busLineCellID =sc.broadcast(busLineInfo.mapValues{//bc(Map[线路号,(起始站点hash,终点站点hash)])
      stopLink=> var cellID = Set.empty[String]
        for(linkInfo<-stopLink)
          cellID = cellID+(linkInfo._1._1,linkInfo._1._2)
        cellID
    }.collectAsMap().toMap)


    // 分割完后规矩6335条 匹配失败112条
    val middleResult = BusTrajecroty.toTrajectory(sc.textFile(gpsFilename),busDirectionMap,busMoblieMap,busLineCellID,size)
      .map{
        x=>
          val busline = busLineDetailInfo.value.getOrElse((x._1,x._3),null)
          if(busline!=null){
            var res = process(x._4,busline,size)
            if(res.isEmpty){
              val busline2 = busLineDetailInfo.value.getOrElse((x._1,math.abs(x._3-1)),null)
              res =  process(x._4,busline2,size)
            }
            (x._1,res)
          }
          else{
            (x._1,process(x._4,busLineDetailInfo.value((x._1,2)),size))
          }
      }.filter(x=>x._2.nonEmpty).map{
      x=>
        var list = List.empty[(String,Long,String,Long)]
        for(i<-1 until x._2.size){
          if(x._2(i-1)._3+1==x._2(i)._3){//解决出行的站点间不相连的情况
            list = (x._2(i-1)._1,x._2(i-1)._2,x._2(i)._1,x._2(i)._2)::list
          }
        }
        (x._1,list)
    }.cache()

    //    val a = middleResult.collect()
    middleResult.flatMap{
      x=>
        var res = List.empty[((String,String,String),Long)]
        var pre = ""
        for(info<-x._2.sortBy(_._4)){
          if(res.isEmpty||res.head._1._3!=info._1){
            res = ((x._1,pre,info._1),info._2)::res
          }
          pre = info._1
          res = ((x._1,pre,info._3),info._4)::res
        }
        res
    }.groupByKey().filter(x=>x._2.size>2).flatMap{
      x=> var res  = List.empty[(String,String,String,Long,Long)]
        val src = x._2.toList.sortBy(x=>x)
        for(i<-1 until src.size){
          res = (x._1._1,x._1._2,x._1._3,src(i-1),src(i) - src(i-1))::res
        }
        //(线路号,上一站,本站,本站到达时间,下辆车到达本站时间)
        res
    }.filter(x=>x._1!="")
      .foreachPartition{
        partitionOfRecords=>
          val ds = new DataSourceUtil
          val conn = ds.getConnection
          conn.setAutoCommit(false);  //设为手动提交
        val psts = conn.prepareStatement("insert into waittime (buslineno, startstop, endstop, arrtime, wtime, curdate) values (?,?,?,?,?,?)")
          val sdf = new SimpleDateFormat("yyyy/MM/dd")
          partitionOfRecords.foreach( record => {
            psts.setString(1,record._1)
            psts.setString(2,record._2)
            psts.setString(3,record._3)
            psts.setString(4,record._4.toString)
            psts.setLong(5, record._5)
            psts.setString(6, sdf.format(new Date(record._4)))
            psts.addBatch()
          })
          psts.executeBatch(); // 执行批量处理
          conn.commit();  // 提交
          conn.close();
      }

    //延误率
    middleResult.flatMap{
      x=> var stopLinkList = List.empty[((String,String,String),(String,Long,String,Long))]
        for(stopLink<-x._2){
          stopLinkList = ((x._1,stopLink._1,stopLink._3),stopLink)::stopLinkList
        }
        stopLinkList
    }.combineByKey(
      x=> x::Nil,
      (list:List[(String,Long,String,Long)],x)=> x::list,
      (list1:List[(String,Long,String,Long)],list2:List[(String,Long,String,Long)])=>list1:::list2
    ).map{
      x=>
        var minTime = Long.MaxValue
        for(linkInfo<-x._2){
          minTime = math.min(linkInfo._4-linkInfo._2,minTime)
        }
        ((x._1._1,minTime,x._1._2,x._1._3),x._2)
    }.flatMap{
      x=> var stopLinkList = List.empty[((String,Int,Long,String,String),(String,Long,String,Long))]
        for(stopLink<-x._2){
          stopLinkList = ((x._1._1,new Date(stopLink._4).getHours,x._1._2,x._1._3,x._1._4),stopLink)::stopLinkList
        }
        stopLinkList
    }.combineByKey(
      x=> x::Nil,
      (list:List[(String,Long,String,Long)],x)=> x::list,
      (list1:List[(String,Long,String,Long)],list2:List[(String,Long,String,Long)])=>list1:::list2
    ).map{
      x=> var time = 0L
        var curdate = new Date()
        var num = 0
        for(linkInfo <- x._2){
          curdate = new Date(linkInfo._4)
          time = time + (linkInfo._4 - linkInfo._2)
          if((linkInfo._4 - linkInfo._2) > x._1._3 * coe){
            num = num + 1
          }
        }
        val sdf = new SimpleDateFormat("yyyy/MM/dd")
        (x._1._1,x._1._4,x._1._5,x._1._3,time/x._2.size,x._1._2,sdf.format(curdate),x._2.size,num.toDouble/x._2.size.toDouble)
    }
      //      .saveAsTextFile("/Users/caojiaqing/Repository/result2")
      .foreachPartition{
      partitionOfRecords=>
        val ds = new DataSourceUtil
        partitionOfRecords.foreach( record => {
          try{
            val conn = ds.getConnection
            conn.setAutoCommit(false);  //设为手动提交
            val stmt = conn.createStatement()
            stmt.addBatch("insert into missrate (buslineno, startstop, endstop, stdtime, avgtime, curtime, curdate, count , rate) values ('"+record._1+"','"+record._2+"','"+record._3+
              "','"+record._4+"','"+record._5+"','"+record._6+"','"+record._7+"','"+record._8+"','"+record._9+"')")
            stmt.executeBatch()
            conn.commit()
            stmt.close()
            conn.close()
          } catch{
            case ex: Exception => {
              println(record.toString())
              println(ex.getCause)
            }
          }

        })
    }

    sc.stop()

  }


  def process(trajecroty: List[(Double,Double,Long)],busline:List[(Double,Double,String,Int)],size:Int): List[(String,Long,Int)] ={
    var cellIDMap = Map.empty[String,(Double,Double,String,Int)]
    var seqNameMap = Map.empty[Int,String]

    for(linkInfo<-busline){
      seqNameMap = seqNameMap + (linkInfo._4->linkInfo._3)
      cellIDMap = cellIDMap + (MapUtil.findCell(linkInfo._1,linkInfo._2,size)->linkInfo)
    }

    val cellIDSet = cellIDMap.keySet
    val temp = trajecroty.filter{
      x=> cellIDSet.contains(MapUtil.findCell(x._1,x._2,size))
    }
    var list = List.empty[(Double,Double,Long,String,Int)]
    var preStop = ""
    for(gps<-temp.sortBy(_._3)){
      val stop = cellIDMap(MapUtil.findCell(gps._1,gps._2,size))
      if(!preStop.equals(stop._3)){
        preStop = stop._3
        list = (gps._1,gps._2,gps._3,stop._3,stop._4)::list
      }else{
        list = (gps._1,gps._2,gps._3,stop._3,stop._4)::list.tail
      }
    }
    list = list.reverse
    var res = List.empty[(String,Long,Int)]
    for(i <- 1 until list.size){
      val interval = list(i)._5-list(i-1)._5
      if(interval==1){
        res = (list(i-1)._4,list(i-1)._3,list(i-1)._5)::res
      }else if(interval<1){
        if(i!=list.size-1){
          return List.empty[(String,Long,Int)]
        }
      }else{
        res = (list(i-1)._4,list(i-1)._3,list(i-1)._5)::res
        val time = (list(i)._3 - list(i-1)._3)/interval
        if(time/1000<600){
          for(j<- 1 until interval){
            res = (seqNameMap(list(i-1)._5+j),list(i-1)._3+j*time,list(i-1)._5+j) :: res
          }
        }
      }
    }
    if(list.nonEmpty){
      res = (list.last._4,list.last._3,list.last._5)::res
      res = res.reverse
      var preList = List.empty[(String,Long,Int)]
      var nextList = List.empty[(String,Long,Int)]
      var nextTime = res.head._2
      var preTime = res.last._2
      for(i <- 1 until  res.head._3){

        if(res.size>1){
          val time = res(1)._2-res.head._2
          if(time/1000<600){
            preList = (seqNameMap(res.head._3-i),nextTime - time,res.head._3-i)::preList
            nextTime = nextTime - time
          }

        }
      }
      res = preList:::res

      for(i<-res.last._3 + 1 to busline.last._4){
        if(res.size>1){
          val time = res.last._2-res(res.size-2)._2
          if(time/1000<600){
            nextList = (seqNameMap(i),preTime + time,i)::nextList
            preTime = preTime + time
          }
        }
      }
      nextList = nextList.reverse
      res = res:::nextList
    }


    var res2 = List.empty[(String,String,Int)]
    for(info<-res){
      res2 = (info._1,new Date(info._2).toString,info._3) :: res2
    }
    res
  }

  private def rad(d: Double): Double={
    d*math.Pi/180.0
  }



}
