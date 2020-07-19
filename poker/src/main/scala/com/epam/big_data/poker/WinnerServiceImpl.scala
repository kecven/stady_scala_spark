package com.epam.big_data.poker

import java.lang.UnsupportedOperationException

import org.apache.spark.rdd.RDD

import scala.math.Ordering.IntOrdering
import scala.util.Try

class WinnerServiceImpl extends WinnerService {
  override def getWinnerName(players: List[Option[Bet]], spectators: List[Option[Bet]]): String = {
    val defaultVal = Some(Bet("Nobody won", 0))

    val winner = Try(players.flatten.maxBy(_.bet)).toOption
      .orElse(Try(spectators.flatten.maxBy(_.bet)).toOption
        .orElse(defaultVal))
      .get


    winner.name;
  }

  def getWinnerNameSpark(players: RDD[Option[Bet]], spectators: RDD[Option[Bet]]): String = {

    val defaultVal:Option[Bet] = Some(Bet("Nobody won", 0))

    val winner:Option[Bet] =
      Try(players
          .filter(_.isDefined)
          .sortBy(_.get.bet, ascending = false)
          .first())
        .toOption
        .getOrElse(Try(spectators
              .filter(_.isDefined)
              .sortBy(_.get.bet, ascending = false)
              .first())
            .toOption
            .getOrElse(defaultVal))

    winner.get.name
  }
}
