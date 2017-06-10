import java.text.SimpleDateFormat
import java.util.Date
import java.util.function.LongFunction

import basic.MapUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._



/**
  * Created by caojiaqing on 15/05/2017.
  */
trait MakeTrajectory {
  def toTrajectory(rdd:RDD[String],busDirectionMap:Broadcast[Map[String,((Double,Double),(Double,Double),Int)]],busMoblieMap:Broadcast[Map[String,String]],busLineCellID: Broadcast[Map[String,Set[String]]],size:Int):RDD[(String,String,Int,List[(Double,Double,Long)])]
}


object BusTrajecroty extends MakeTrajectory {

  case class BusGps(id: String,gpsDate:String,gpsMonitorId:String,gpsDataSequence:String,gpsCmd:String,gpsHHMMSS:String,validateFlag:String,
                    lat:String,lon:String,speed:String,direction:String,gpsDDMMYY:String,altitude:String,vehicleStatus:String,
                    oilScale:String,temp:String,humidity:String,course:String,location:String,var gpsSendDate:String,sendTimeStamp:String){

  }

  //返回RDD[手机号，List[(lat,lng,time)]]
  override def toTrajectory(rdd: RDD[String],busDirectionMap:Broadcast[Map[String,((Double,Double),(Double,Double),Int)]],busMoblieMap:Broadcast[Map[String,String]],busLineCellID: Broadcast[Map[String,Set[String]]],size:Int): RDD[(String,String,Int,List[(Double,Double,Long)])] = {

    rdd.map(line=>json2Map(line)).map(gps=>(gps.gpsMonitorId,(gps.lon.toDouble,gps.lat.toDouble,gps.gpsSendDate.toLong))).combineByKey(
      x=> x::Nil,
      (list:List[(Double,Double,Long)],x)=> x::list,
      (list1:List[(Double,Double,Long)],list2:List[(Double,Double,Long)])=> list1:::list2
    ).mapValues(
      gpsList=> gpsList.sortBy(_._3)
    ).filter(x=>x._2!=null&&x._2.length>3)
      .flatMap{//采样点间时间间隔超过1h 将轨迹分割 距离过远的切割
        x=>
          var res = List.empty[(String,List[(Double,Double,Long)])]
          var list = List.empty[(Double,Double,Long)]
          for(i<-1 until x._2.size){
            if((x._2(i)._3-x._2(i-1)._3)/1000>3600){
              list=x._2(i-1)::list
              res = (x._1,list.reverse)::res
              list = List.empty[(Double,Double,Long)]
            }else{
              list = x._2(i-1)::list
            }
          }
          list=x._2.last::list
          res = (x._1,list.reverse)::res
          res
      }
      .filter(x=>x._2!=null&&x._2.length>3)//过滤掉采样点少于3个的轨迹
      .map{
      x=>
        val lineNo = busMoblieMap.value.getOrElse(x._1,"-1")
        ((lineNo,x._1),x._2)//((线路号，手机号),gps采样点)
    }
      .filter(x=>x._1._1!="-1")//删除没有对应线路号的轨迹
      .flatMap{//轨迹切割
      x =>
        val stopTuple = busDirectionMap.value.getOrElse(x._1._1, null)
        var temp = List.empty[String]
        val list = x._2.map{
          y=>
            var stopCellID = ""

            if(busLineCellID.value(x._1._1).contains(MapUtil.findCell(y._1,y._2,size))){
              stopCellID = MapUtil.findCell(y._1,y._2,size)

            }
            temp = MapUtil.findCell(y._1,y._2,size)::temp
            (y._1,y._2,y._3,stopCellID)
        }
        //
        val res = splitTrajectory_precise(x._1,list,stopTuple,size)

        res
    }.filter{
      x=>
        var count = 0
        x._4.foreach{
          y=>
            if(busLineCellID.value(x._1).contains(MapUtil.findCell(y._1,y._2,size))){
              count = count + 1
            }
        }
        count>1
    }


    //    val aa =   rdd.map(line=>json2Map(line)).map(gps=>(gps.gpsMonitorId,(gps.lon.toDouble,gps.lat.toDouble,gps.gpsSendDate.toLong))).combineByKey(
    //      x=> x::Nil,
    //      (list:List[(Double,Double,Long)],x)=> x::list,
    //      (list1:List[(Double,Double,Long)],list2:List[(Double,Double,Long)])=> list1:::list2
    //    ).mapValues(
    //      gpsList=> gpsList.sortBy(_._3)
    //    ).filter(x=>x._2!=null&&x._2.length>3)
    //      .flatMap{//采样点间时间间隔超过1h 将轨迹分割 距离过远的切割
    //        x=>
    //          var res = List.empty[(String,List[(Double,Double,Long)])]
    //          var list = List.empty[(Double,Double,Long)]
    //          for(i<-1 until x._2.size){
    //            if((x._2(i)._3-x._2(i-1)._3)/1000>3600){
    //              list=x._2(i-1)::list
    //              res = (x._1,list.reverse)::res
    //              list = List.empty[(Double,Double,Long)]
    //            }else{
    //              list = x._2(i-1)::list
    //            }
    //          }
    //          list=x._2.last::list
    //          res = (x._1,list.reverse)::res
    //          res
    //      }
    //      .filter(x=>x._2!=null&&x._2.length>3)//过滤掉采样点少于3个的轨迹
    //      .map{
    //      x=>
    //        val lineNo = busMoblieMap.value.getOrElse(x._1,"-1")
    //        ((lineNo,x._1),x._2)//((线路号，手机号),gps采样点)
    //    }
    //      .filter(x=>x._1._1!="-1")//删除没有对应线路号的轨迹
    //      .flatMap{//轨迹切割
    //      x =>
    //        val stopTuple = busDirectionMap.value.getOrElse(x._1._1, null)
    //        var temp = List.empty[String]
    //        val list = x._2.map{
    //          y=>
    //            var stopCellID = ""
    //
    //            if(busLineCellID.value(x._1._1).contains(MapUtil.findCell(y._1,y._2,size))){
    //              stopCellID = MapUtil.findCell(y._1,y._2,size)
    //
    //            }
    //            temp = MapUtil.findCell(y._1,y._2,size)::temp
    //            (y._1,y._2,y._3,stopCellID)
    //        }
    //        var long = ""
    //        for(gps<-list){
    //          long = long + "{" +"\r\n"
    //
    //          long = long + ("lat:"+gps._2+",") + "\r\n"
    //          long = long + ("lng:"+gps._1)  +"\r\n"
    //          long = long + ("},")  +"\r\n"
    //        }
    //        val res = splitTrajectory_precise(x._1,list,stopTuple,size)
    //
    //        for(short<-res){
    //          var shortString = ""
    //          for(gps<-short._4){
    //            shortString = shortString + ("{")  + "\r\n"
    //            shortString = shortString +("lat:"+gps._2+",") + "\r\n"
    //            shortString = shortString +("lng:"+gps._1)+ "\r\n"
    //            shortString = shortString +("},")+ "\r\n"
    //          }
    //          println("*********************************")
    //        }
    //
    //        res
    //    }.filter{
    //      x=>
    //        var count = 0
    //        x._4.foreach{
    //          y=>
    //            if(busLineCellID.value(x._1).contains(MapUtil.findCell(y._1,y._2,size))){
    //              count = count + 1
    //            }
    //        }
    //        count>1
    //    }
    //      .collect()
    //数据插值
    //      .map{
    //      x=> var list = List.empty[(Double,Double,Long)]
    //        for(i<- 1 until x._2.length){
    //          val interval = (x._2(i)._3-x._2(i-1)._3)/1000
    //          if(interval<5)
    //            list = x._2(i-1)::list
    //          else{
    //            //5秒插值
    //            val times = interval.toInt/5
    //            val time = interval.toInt/(times+1)
    //            val lat = (x._2(i)._1-x._2(i-1)._1)/(times+1)
    //            val lng = (x._2(i)._2-x._2(i-1)._2)/(times+1)
    //            list = x._2(i-1)::list
    //            for(j<- 1 to times){
    //              list = (x._2(i-1)._1+j*lat,x._2(i-1)._2+j*lng,x._2(i-1)._3+j*time*1000)::list
    //            }
    //          }
    //        }
    //        (x._1,list.reverse)
    //    }

  }

  def json2Map(line: String):BusGps =  {
    implicit val formats = DefaultFormats
    //解析结果
    val busgps: BusGps = parse(line).extract[BusGps]
    val sdf = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss a")
    busgps.gpsSendDate = sdf.parse(busgps.gpsSendDate).getTime.toString
    busgps
  }

  /**
    *
    * @param info (线路号,手机号)
    * @param longTrajectory（gps采样点）
    * @param stopTuple((slng,slat),(elng,elat),direction)
    * @return List[(线路号,手机号,上下行标志,List[(lng,lat,Long)])]
    */
  def splitTrajectory_precise(info:(String,String),longTrajectory:(List[(Double,Double,Long,String)]),stopTuple:((Double,Double),(Double,Double),Int),size:Int):List[(String,String,Int,List[(Double,Double,Long)])]={
    var i = 0
    var list = List.empty[(Double,Double,Long)]
    var result = List.empty[(String,String,Int,List[(Double,Double,Long)])]
    var pre = ""
    var preList = List.empty[(Double,Double,Long)]
    var nextList = List.empty[(Double,Double,Long)]

    var gps1:(Double,Double,Long,String) = null
    var gps2:(Double,Double,Long,String) = null
    var gps3:(Double,Double,Long,String) = null

    var directive = -1

    while (i<longTrajectory.size){
      if(gps3!=null){
        preList = nextList
        nextList = List.empty[(Double,Double,Long)]
        gps1 = gps2
        gps2 = gps3
        pre = gps2._4
        gps3 = null
      }
      if(gps1==null){
        while (i<longTrajectory.size&&longTrajectory(i)._4.equals("")){
          i=i+1
        }
        if(i>=longTrajectory.size)
          return result
        gps1 = longTrajectory(i)
        pre=gps1._4
      }
      if(gps2 == null){
        while (i<longTrajectory.size&&(longTrajectory(i)._4.equals("")||longTrajectory(i)._4.equals(pre))){
          preList = (longTrajectory(i)._1,longTrajectory(i)._2,longTrajectory(i)._3)::preList
          i=i+1
        }
        if(i>=longTrajectory.size)
          return result
        preList = (longTrajectory(i)._1,longTrajectory(i)._2,longTrajectory(i)._3)::preList
        preList = preList.reverse
        gps2 = longTrajectory(i)
        pre = gps2._4
      }
      if(gps3==null){
        while (i<longTrajectory.size&&(longTrajectory(i)._4.equals("")||longTrajectory(i)._4.equals(pre))){
          nextList = (longTrajectory(i)._1,longTrajectory(i)._2,longTrajectory(i)._3)::nextList
          i=i+1
        }
        if(i>=longTrajectory.size)
          return result
        nextList = (longTrajectory(i)._1,longTrajectory(i)._2,longTrajectory(i)._3)::nextList
        nextList = nextList.reverse
        gps3 = longTrajectory(i)
        pre = gps3._4
      }

      val distance1 = MapUtil.calPointDistance(gps1._1,gps1._2,stopTuple._2._1,stopTuple._2._2)
      val distance2 = MapUtil.calPointDistance(gps2._1,gps2._2,stopTuple._2._1,stopTuple._2._2)
      val distance3 = MapUtil.calPointDistance(gps3._1,gps3._2,stopTuple._2._1,stopTuple._2._2)

      if(distance2<distance1&&distance3<distance2){
        if(directive==0){
          list = list:::preList
          preList = List.empty[(Double,Double,Long)]
        }else if(directive==1){
          result = (info._1,info._2,1,list)::result
          list = List.empty[(Double,Double,Long)]
          gps1 = null
          gps2 = null
          gps3 = null
        }else{
          //误差
          //list = list:::preList
          preList = List.empty[(Double,Double,Long)]
        }
        directive = 0
      }else if(distance2>distance1&&distance3>distance2){
        if(directive==1){
          list = list:::preList
          preList = List.empty[(Double,Double,Long)]
        }else if(directive==0){
          result = (info._1,info._2,0,list)::result
          list = List.empty[(Double,Double,Long)]
          gps1 = null
          gps2 = null
          gps3 = null
        }else{
          //误差
          //          list = list:::preList
          preList = List.empty[(Double,Double,Long)]
        }
        directive = 1
      }else{

        if(directive==0){
          list = nextList:::preList:::list //误差
          result = (info._1,info._2,0,list.sortBy(_._3))::result
        }else if(directive==1){
          list = nextList:::preList:::list //误差
          result = (info._1,info._2,1,list.sortBy(_._3))::result
        }
        nextList = List.empty[(Double,Double,Long)]
        preList = List.empty[(Double,Double,Long)]
        list = List.empty[(Double,Double,Long)]
        gps1 = null
        gps2 = null
        gps3 = null
      }

    }
    result
  }



  def splitTrajectory(info:(String,String),longTrajectory:(List[(Double,Double,Long,String)]),stopTuple:((Double,Double),(Double,Double),Int),size:Int):List[(String,String,Int,List[(Double,Double,Long)])]={
    var i = 0
    var list = List.empty[(Double,Double,Long)]
    var result = List.empty[(String,String,Int,List[(Double,Double,Long)])]
    var pre = ""
    var preList = List.empty[(Double,Double,Long)]
    var nextList = List.empty[(Double,Double,Long)]

    var gps1:(Double,Double,Long,String) = null
    var gps2:(Double,Double,Long,String) = null
    var gps3:(Double,Double,Long,String) = null

    var directive = -1

    while (i<longTrajectory.size){
      if(gps3!=null){
        preList = nextList
        nextList = List.empty[(Double,Double,Long)]
        gps1 = gps2
        gps2 = gps3
        pre = gps2._4
        gps3 = null
      }
      if(gps1==null){
        while (i<longTrajectory.size&&longTrajectory(i)._4.equals("")){
          i=i+1
        }
        if(i>=longTrajectory.size)
          return result
        gps1 = longTrajectory(i)
        pre=gps1._4
      }
      if(gps2 == null){
        while (i<longTrajectory.size&&(longTrajectory(i)._4.equals("")||longTrajectory(i)._4.equals(pre))){
          preList = (longTrajectory(i)._1,longTrajectory(i)._2,longTrajectory(i)._3)::preList
          i=i+1
        }
        if(i>=longTrajectory.size)
          return result
        preList = (longTrajectory(i)._1,longTrajectory(i)._2,longTrajectory(i)._3)::preList
        preList = preList.reverse
        gps2 = longTrajectory(i)
        pre = gps2._4
      }
      if(gps3==null){
        while (i<longTrajectory.size&&(longTrajectory(i)._4.equals("")||longTrajectory(i)._4.equals(pre))){
          nextList = (longTrajectory(i)._1,longTrajectory(i)._2,longTrajectory(i)._3)::nextList
          i=i+1
        }
        if(i>=longTrajectory.size)
          return result
        nextList = (longTrajectory(i)._1,longTrajectory(i)._2,longTrajectory(i)._3)::nextList
        nextList = nextList.reverse
        gps3 = longTrajectory(i)
        pre = gps3._4
      }

      val distance1 = MapUtil.calPointDistance(gps1._1,gps1._2,stopTuple._2._1,stopTuple._2._2)
      val distance2 = MapUtil.calPointDistance(gps2._1,gps2._2,stopTuple._2._1,stopTuple._2._2)
      val distance3 = MapUtil.calPointDistance(gps3._1,gps3._2,stopTuple._2._1,stopTuple._2._2)

      if(distance2<distance1&&distance3<distance2){
        if(directive==0){
          list = list:::preList
          preList = List.empty[(Double,Double,Long)]
        }else if(directive==1){
          result = (info._1,info._2,1,list)::result
          list = List.empty[(Double,Double,Long)]
          gps1 = null
          gps2 = null
          gps3 = null
        }
        directive = 0
      }else if(distance2>distance1&&distance3>distance2){
        if(directive==1){
          list = list:::preList
          preList = List.empty[(Double,Double,Long)]
        }else if(directive==0){
          result = (info._1,info._2,0,list)::result
          list = List.empty[(Double,Double,Long)]
          gps1 = null
          gps2 = null
          gps3 = null
        }
        directive = 1
      }else{

        if(directive==0){
          list = nextList:::preList:::list
          nextList = List.empty[(Double,Double,Long)]
          preList = List.empty[(Double,Double,Long)]
          result = (info._1,info._2,0,list)::result
          list = List.empty[(Double,Double,Long)]

        }else if(directive==1){
          list = nextList:::preList:::list
          nextList = List.empty[(Double,Double,Long)]
          preList = List.empty[(Double,Double,Long)]
          result = (info._1,info._2,1,list)::result
          list = List.empty[(Double,Double,Long)]
        }
      }

    }

    result
  }


}
