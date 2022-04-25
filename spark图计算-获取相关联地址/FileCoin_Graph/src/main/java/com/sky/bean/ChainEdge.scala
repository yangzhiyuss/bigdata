package com.sky.bean

case class ChainEdge(from: String, to: String, total_value: Option[Double],
               max_value: Option[Double], min_value: Option[Double], total_gas_consume: Option[Double],
                count: Option[Long], last_time: String, start_time: String);
