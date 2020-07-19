package com.epam.big_data.poker

import java.util.Comparator

case class Bet(name: String, bet:Int) extends Comparator[Bet] with Serializable {
  override def compare(x: Bet, y: Bet): Int = {
    Ordering[Int].compare(x.bet, y.bet)
  }
}
