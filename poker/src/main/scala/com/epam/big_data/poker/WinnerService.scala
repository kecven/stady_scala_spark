package com.epam.big_data.poker

import org.apache.spark.rdd.RDD

trait WinnerService {

  def getWinnerName(players: List[Option[Bet]], spectators: List[Option[Bet]]): String;
  def getWinnerNameSpark(players: RDD[Option[Bet]], spectators: RDD[Option[Bet]]): String;

}
