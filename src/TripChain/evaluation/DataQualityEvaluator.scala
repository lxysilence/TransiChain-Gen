package TripChain.evaluation

import common.CommonScalaMethod
import utils.SystemUtils

object DataQualityEvaluator {

  //北斗集群中数据的路径
  //由于BfmapReader读取地图文件使用的是new FileInputStream方法， 故只能使用yarn-client模式运行程序，且文件路径需绝对路径
  //如果要用yarn-cluster模式，就只能手动重写BfmapReader类再打包安装依赖
  var mapPath = "/root/lxying/map/overpass.bfmap"
  var synDataPathPrefix = "/user/lxying/RCDPjour/genData"
  var commuteDataPrefix = "/user/lxying/RCDPjour/commuteData"
  var evaluaDataPrefix = "/user/lxying/RCDPjour/evaluation"
  var regionDataPath = "/user/lxying/RCDPjour/sz_points_v3.csv"


  ////  中心集群中数据的路径
  //      val mapPath = "/home/frzheng/lxying_files/projects/map/map.bfmap"
  //      val dataPathPrefix = "/user/lxying/testData/taxiGps/"
  //      var resultPathPrefix = "/user/lxying/resultData/GetMaxErrorRoad/"


  //本地数据路径
  if (SystemUtils.isWindows) {
    synDataPathPrefix = "D:\\traffic\\TripChain\\experientData\\ExperimentData\\Adatrace\\part-00000-eps2.0-iteration0.dat"
    commuteDataPrefix = "D:\\traffic\\TripChain\\experientData\\ExperimentData\\Adatrace\\part-00000"
    evaluaDataPrefix = "D:\\traffic\\TripChain\\experientData\\evaluation"
    regionDataPath = "output/sz_points_v3.csv"
  }


//  if (SystemUtils.isWindows) {
//    synDataPathPrefix = "D:\\experiment\\RCDPjour\\commuteData\\Adatrace\\20\\part*"
//    commuteDataPrefix = "D:\\experiment\\RCDPjour\\commuteData\\Adatrace\\99\\part*"
//    evaluaDataPrefix = "D:\\experiment\\RCDPjour\\evaluation"
//    regionDataPath = "output/sz_points_v3.csv"
//  }
  def main(args: Array[String]): Unit = {

    val sc = CommonScalaMethod.buildSparkContext("pro_APP")

    try{

//      //线层面相关评估
//
//      //KT_station
////      KT.AdaKT_s(
////        sc,
////        s"$synDataPathPrefix",       // 原始trip数据路径
////        s"$commuteDataPrefix",
////      )
//      KT.CalKT_h(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )
//
//
//      //MRE_station
//      AvQE.CalAvQE_s(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )
//
//      //KT_h
//      KT.AdaKT_st(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )
//
//      //MRE_st
//      AvQE.CalAvQE_st(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )
//
//      //CalJSD_st
//      JSD.CalJSD_st(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )
//
//      //AdaKT_subpaths
//      KT.CalKT_r(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )
//      //AdaKT_subpaths
//      AvQE.CalAvQE_r(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )

//      //JSD_tripLength
//      JSD.CalJSD_tripLength1(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )

//      //JSD_OD
//      JSD.CalOD_CorrelationError(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )
//
//      //CalAvQE_temporal_subpaths
//      AvQE.CalAvQE_temporal_subpaths(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )
//
//      //JSD_OD,t
//      JSD.CalJSD_CorrelationError(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )


      //JSD_OD,t
//      privacy.infer(
//        sc,
//        s"$synDataPathPrefix"      // 原始trip数据路径
//      )

      privacy.Ada_infer(
        sc,
        s"$synDataPathPrefix"      // 原始trip数据路径
      )


      println("**********已经完成所有程序的运行************")
    }finally {
      sc.stop()
    }
  }


}
