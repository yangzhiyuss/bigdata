package com.sky.ML

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.sky.bean.{ChainEdge, ChainVertex}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object FilcoinGraph {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("FilcoinGraph")
      .config("spark.sql.warehouse.dir", "hdfs://server-1:8020/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    //获取边数据
    val edgeSource: DataFrame = spark.sql("select * from blockfilecoin.edg_table")

    //获取点的数据
    val vertexSource: DataFrame = spark.sql("select * from blockfilecoin.vertex")

    //边和节点转化为rdd
    val edgeRDD: RDD[ChainEdge] = edgeSource.as[ChainEdge].rdd

    val vertexRDD: RDD[ChainVertex] = vertexSource.as[ChainVertex].rdd

    //过滤不合要求的边
    val edgeFilter: RDD[ChainEdge] = edgeRDD.filter(data => {
      val seed: Double = Math.pow(10, -18)
      val max_value: Double = data.max_value.getOrElse(0.0)
      val total_value: Double = data.total_value.getOrElse(0.0)

      //把总共转账少于100FIL和最大转账少于10FIL的边过滤，为非洗钱边
      if (max_value * seed >= 10 && total_value * seed >= 100 ) {
        true
      } else {
        false
      }
    })

    //将边数据转的from 和 to 转化为hash值
    val edgeMap: RDD[(VertexId, VertexId, ChainEdge)] = edgeFilter.map(data => {
      val seed: Double = Math.pow(10, -18)
      var total_value: Double = data.total_value.getOrElse(0.0)
      var max_value: Double = data.max_value.getOrElse(0.0)
      var min_value: Double = data.min_value.getOrElse(0.0)
      var total_gas_consume: Double = data.total_gas_consume.getOrElse(0.0)
      (HashUtil.getHash(data.from), HashUtil.getHash(data.to),
        ChainEdge(data.from, data.to, Some(total_value * seed), Some(max_value * seed),
          Some(min_value * seed), Some(total_gas_consume * seed),
          data.count, data.last_time, data.start_time))
    })

    //建立图的边
    val edge: RDD[Edge[ChainEdge]] = edgeMap.map(data => Edge(data._1, data._2, data._3))

    //过滤不和要求的节点
    val vertexFilter: RDD[ChainVertex] = vertexRDD.filter(data => {
      val seed: Double = Math.pow(10, -18)
      var get_max_value: Double = data.get_max_value.getOrElse(0.0)
      var get_total_value: Double = data.get_total_value.getOrElse(0.0)
      var pay_max_value: Double = data.pay_max_value.getOrElse(0.0)
      var pay_total_value: Double = data.pay_total_value.getOrElse(0.0)
      val extra_money: Double = data.extra_money.getOrElse(0.0)

      //假如节点的收入和转出的FIL都小于500FIL，最大交易额度小于10FIL的节点过滤， 为非洗钱节点
      if ((get_max_value * seed >= 10 && get_total_value * seed >= 500)
        || (pay_max_value * seed >= 10 && pay_total_value * seed >= 500)) {
        true
      } else {
        false
      }
    })

    //将节点的地址转化为hash值
    val vertex: RDD[(VertexId, ChainVertex)] = vertexFilter.map(data => {
      var seed: Double = Math.pow(10, -18)
      var get_max_value: Double = data.get_max_value.getOrElse(0.0)
      var get_total_value: Double = data.get_total_value.getOrElse(0.0)
      var pay_max_value: Double = data.pay_max_value.getOrElse(0.0)
      var pay_total_value: Double = data.pay_total_value.getOrElse(0.0)
      var extra_money: Double = data.extra_money.getOrElse(0.0)
      var pay_total_gas_consume: Double = data.pay_total_gas_consume.getOrElse(0.0)

      (HashUtil.getHash(data.filecoin_address), ChainVertex(data.filecoin_address, Some(get_total_value * seed),
        Some(get_max_value * seed), Some(pay_total_value * seed), Some(pay_max_value * seed),
        Some(extra_money * seed), Some(pay_total_gas_consume * seed), data.inDegree,
        data.outDegree, data.get_address_count, data.pay_address_count, data.get_start_time,
        data.pay_start_time, data.get_last_time, data.pay_last_time))
    })

    //建立ChainGraph图
    val chainGrap: Graph[ChainVertex, ChainEdge] = Graph(vertex, edge)

    //获取连通图
    val verticesConnectedGraph: VertexRDD[VertexId] = chainGrap.connectedComponents().vertices

    //连接节点的信息
    val verticesConnectedJoin: RDD[(VertexId, (VertexId, ChainVertex))] = verticesConnectedGraph.join(vertex)

    //将数据转化为（连通图ID, 节点ID, 节点信息...)
    val verticesConnectedJoinResult: RDD[(VertexId, VertexId, String, Double, Double, Double, Double, Double, Double, VertexId, VertexId, VertexId, VertexId, String, String, String, String)] = verticesConnectedJoin.map(data => (
      data._2._1, data._1, data._2._2.filecoin_address,
      data._2._2.get_total_value.getOrElse(0.0), data._2._2.get_max_value.getOrElse(0.0),
      data._2._2.pay_total_value.getOrElse(0.0), data._2._2.pay_max_value.getOrElse(0.0),
      data._2._2.extra_money.getOrElse(0.0), data._2._2.pay_total_gas_consume.getOrElse(0.0),
      data._2._2.inDegree.getOrElse(0L), data._2._2.outDegree.getOrElse(0L),
      data._2._2.get_address_count.getOrElse(0L), data._2._2.pay_address_count.getOrElse(0),
      data._2._2.get_start_time, data._2._2.pay_start_time,
      data._2._2.get_last_time, data._2._2.pay_last_time))

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS  blockfilecoin.Vertex_ConnectedGraph(
        |  `graph_id`  bigint,
        |  `address_hash` bigint,
        |  `filecoin_address` String,
        |  `get_total_value` double,
        |  `get_max_value` double,
        |  `pay_total_value` double,
        |  `pay_max_value` double,
        |  `extra_money` double,
        |  `pay_total_gas_consume` double,
        |  `indegree` bigint,
        |  `outdegree` bigint,
        |  `get_address_count` bigint,
        |  `pay_address_count` bigint,
        |  `get_start_time` String,
        |  `pay_start_time` String,
        |  `get_last_time` String,
        |  `pay_last_time` String
        |  )
      """.stripMargin)

    // Turn on flag for Hive Dynamic Partitioning
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    // Create a Hive partitioned table using DataFrame API

    verticesConnectedJoinResult.toDS().createOrReplaceTempView("tmp_table")


    spark.sql(
      """
        |insert into blockfilecoin.Vertex_ConnectedGraph
        |select *
        |from tmp_table
      """.stripMargin)

      //    vertexConnectGraphById.saveAsTextFile("./result.txt")

      spark.close()
  }

}
