package com.sky.ML

object HashUtil {
  def getHash(str: String): Long = {
    val seed:Int  = 131 // 31 131 1313 13131 131313 etc..
    var hash:Long  = 0
    for(i <- 0 until str.length){
      hash = hash * seed + str.charAt(i)
      hash = hash & 0x7FFFFFFFFFFFFFFFL
    }
    return hash
  }
}
