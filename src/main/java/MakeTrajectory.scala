import java.text.SimpleDateFormat

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._



/**
  * Created by caojiaqing on 15/05/2017.
  */
trait MakeTrajectory {
  def toTrajectory(rdd:RDD[String],busDirectionMap:Broadcast[Map[String,((Double,Double),(Double,Double),Int)]],):RDD[(String,List[(Double,Double,Long)])]
}


object BusTrajecroty extends MakeTrajectory{

  case class BusGps(id: String,gpsDate:String,gpsMonitorId:String,gpsDataSequence:String,gpsCmd:String,gpsHHMMSS:String,validateFlag:String,
                    lat:String,lon:String,speed:String,direction:String,gpsDDMMYY:String,altitude:String,vehicleStatus:String,
                    oilScale:String,temp:String,humidity:String,course:String,location:String,var gpsSendDate:String,sendTimeStamp:String){

  }

  //返回RDD[手机号，List[(lat,lng,time)]]
  override def toTrajectory(rdd: RDD[String],busDirectionMap:Broadcast[Map[String,((Double,Double),(Double,Double),Int)]]): RDD[(String, List[(Double, Double, Long)])] = {

    rdd.map(line=>json2Map(line)).map(gps=>(gps.gpsMonitorId,(gps.lat.toDouble,gps.lon.toDouble,gps.gpsSendDate.toLong))).combineByKey(
      x=> x::Nil,
      (list:List[(Double,Double,Long)],x)=> x::list,
      (list1:List[(Double,Double,Long)],list2:List[(Double,Double,Long)])=> list1:::list2
    ).mapValues(
      gpsList=> gpsList.sortBy(_._3)
    ).filter(x=>x._2!=null&&x._2.length>1)
      .flatMap{
        x=>

          splitTrajectory(x,busDirectionMap)
      }
      //数据插值
      .map{
      x=> var list = List.empty[(Double,Double,Long)]
        for(i<- 1 until x._2.length){
          val interval = (x._2(i)._3-x._2(i-1)._3)/1000
          if(interval<5)
            list = x._2(i-1)::list
          else{
            //5秒插值
            val times = interval.toInt/5
            val time = interval.toInt/(times+1)
            val lat = (x._2(i)._1-x._2(i-1)._1)/(times+1)
            val lng = (x._2(i)._2-x._2(i-1)._2)/(times+1)
            list = x._2(i-1)::list
            for(j<- 1 to times){
              list = (x._2(i-1)._1+j*lat,x._2(i-1)._2+j*lng,x._2(i-1)._3+j*time*1000)::list
            }
          }
        }
        (x._1,list.reverse)
    }
  }

  def json2Map(line: String):BusGps =  {
    implicit val formats = DefaultFormats
    //解析结果
    val busgps: BusGps = parse(line).extract[BusGps]
    val sdf = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss a")
    busgps.gpsSendDate = sdf.parse(busgps.gpsSendDate).getTime.toString
    busgps
  }

   def splitTrajectory(longTrajectory:(String,List[(Double,Double,Long)]),busDirectionMap:Broadcast[Map[String,((Double,Double),(Double,Double),Int)]]):List[(String,List[(Double,Double,Long)])]={
     for()
     null
  }
}
