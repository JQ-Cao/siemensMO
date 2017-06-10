import basic.MapUtil
import org.apache.spark.rdd.RDD


/**
  * Created by caojiaqing on 17/05/2017.
  */
object BusLineInfo {
  //RDD(线路号，Map[(起始站点hash,终点站点hash),(起始站点名，终点站点名)])
  def getBusLineInfo(lines:RDD[String],size:Int):RDD[(String,Map[(String,String),(String,String)])]={
    //
    lines.map{
      x=>val arr = x.replaceAll("\\]","").split(",")
        ((arr(1),arr(6)),(arr(3),arr(4),arr(5),arr(7).toInt))
    }.combineByKey(
      x=> x::Nil,
      (list:List[(String,String,String,Int)],x)=> x::list,
      (list1:List[(String,String,String,Int)],list2:List[(String,String,String,Int)])=>list1:::list2
    ).map{
      x=>
        var stopList = List.empty[(String,String)]
        for(stopInfo<-x._2.sortBy(_._4)){
          stopList = (MapUtil.findCell(stopInfo._3.toDouble,stopInfo._2.toDouble,size),stopInfo._1) :: stopList
        }
        (x._1._1,stopList.reverse)
    }.groupByKey().map{
      busLineInfo=>
        var stopLinkInfo = Map.empty[(String,String),(String,String)]//[(起始站点hash,终点站点hash),(起始站点名,终点站点名)]
        for(stopList<-busLineInfo._2){
          for(i<-1 until stopList.length){
            stopLinkInfo = stopLinkInfo+(((stopList(i-1)._1,stopList(i)._1),(stopList(i-1)._2,stopList(i)._2)))
          }
        }
        (busLineInfo._1,stopLinkInfo)
    }
  }

  def getBusLineDetailInfo(line:RDD[String]):RDD[((String,Int),List[(Double,Double,String,Int)])] = {
    line.map{
      x=>val arr = x.replaceAll("\\]","").split(",")
        ((arr(1),arr(6).toInt),(arr(5).toDouble,arr(4).toDouble,arr(3),arr(7).toInt))//((线路名,方向标志),(lng,lat,站点序列))
    }.combineByKey(
      x=> x::Nil,
      (list:List[(Double,Double,String,Int)],x)=> x::list,
      (list1:List[(Double,Double,String,Int)],list2:List[(Double,Double,String,Int)])=>list1:::list2
    ).map{
      x=> (x._1,x._2.sortBy(_._4))
    }
  }

  /**
    *
    * @param lines
    * @return RDD[(线路号,（(起点站lng,起点站lat),(终点站lng,终点站lat),上下行标志）)]
    */
  def getBusLineDirectionInfo(lines:RDD[String]): RDD[(String,((Double,Double),(Double,Double),Int))] ={
    lines.map{
      x=>val arr = x.replaceAll("\\]","").split(",")
        ((arr(1),arr(6).toInt),(arr(5).toDouble,arr(4).toDouble,arr(7).toInt))//((线路名,方向标志),(lng,lat,站点序列))
    }.combineByKey(
      x=> x::Nil,
      (list:List[(Double,Double,Int)],x)=> x::list,
      (list1:List[(Double,Double,Int)],list2:List[(Double,Double,Int)])=>list1:::list2
    ).map{
      x=>val stopList = x._2.sortBy(_._3)
        val firstStop = stopList.head
        val lastStop = stopList.last
        (x._1._1,((firstStop._1,firstStop._2),(lastStop._1,lastStop._2),x._1._2))
    }
  }

  def getBusMoblieInfo(lines:RDD[String]):RDD[(String,String)]={
    lines.map{
      line=>
        val arr = line.split(",")
        (arr(4).replaceAll("\\]",""),arr(3))
    }
  }
}
