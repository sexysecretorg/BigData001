package com.zsr.dmp.tools.utils

import org.apache.spark.sql.Row

object RowUtil {
  def getRow(arr:Array[String]):Row ={
    Row(
      arr(0),
      S2IOD.toInt(arr(1)),
      S2IOD.toInt(arr(2)),
      S2IOD.toInt(arr(3)),
      S2IOD.toInt(arr(4)),
      arr(5),
      arr(6),
      S2IOD.toInt(arr(7)),
      S2IOD.toInt(arr(8)),
      S2IOD.toDouble(arr(9)),
      S2IOD.toDouble(arr(10)),
      arr(11),
      arr(12),
      arr(13),
      arr(14),
      arr(15),
      arr(16),
      S2IOD.toInt(arr(17)),
      arr(18),
      arr(19),
      S2IOD.toInt(arr(20)),
      S2IOD.toInt(arr(21)),
      arr(22),
      arr(23),
      arr(24),
      arr(25),
      S2IOD.toInt(arr(26)),
      arr(27),
      S2IOD.toInt(arr(28)),
      arr(29),
      S2IOD.toInt(arr(30)),
      S2IOD.toInt(arr(31)),
      S2IOD.toInt(arr(32)),
      arr(33),
      S2IOD.toInt(arr(34)),
      S2IOD.toInt(arr(35)),
      S2IOD.toInt(arr(36)),
      arr(37),
      S2IOD.toInt(arr(38)),
      S2IOD.toInt(arr(39)),
      S2IOD.toDouble(arr(40)),
      S2IOD.toDouble(arr(41)),
      S2IOD.toInt(arr(42)),
      arr(43),
      S2IOD.toDouble(arr(44)),
      S2IOD.toDouble(arr(45)),
      arr(46),
      arr(47),
      arr(48),
      arr(49),
      arr(50),
      arr(51),
      arr(52),
      arr(53),
      arr(54),
      arr(55),
      arr(56),
      S2IOD.toInt(arr(57)),
      S2IOD.toDouble(arr(58)),
      S2IOD.toInt(arr(59)),
      S2IOD.toInt(arr(60)),
      arr(61),
      arr(62),
      arr(63),
      arr(64),
      arr(65),
      arr(66),
      arr(67),
      arr(68),
      arr(69),
      arr(70),
      arr(71),
      arr(72),
      S2IOD.toInt(arr(73)),
      S2IOD.toDouble(arr(74)),
      S2IOD.toDouble(arr(75)),
      S2IOD.toDouble(arr(76)),
      S2IOD.toDouble(arr(77)),
      S2IOD.toDouble(arr(78)),
      arr(79),
      arr(80),
      arr(81),
      arr(82),
      arr(83),
      S2IOD.toInt(arr(84)))
  }

}