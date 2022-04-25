package com.sky.bean

case class ChainVertex(filecoin_address: String, get_total_value: Option[Double],
                  get_max_value: Option[Double], pay_total_value: Option[Double],
                  pay_max_value: Option[Double], extra_money:Option[Double],
                  pay_total_gas_consume: Option[Double], inDegree: Option[Long],
                  outDegree: Option[Long], get_address_count: Option[Long],
                  pay_address_count: Option[Long], get_start_time: String,
                  pay_start_time: String, get_last_time: String,
                  pay_last_time: String);



