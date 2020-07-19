package com.epam.big_data.poker

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Assert, Before, BeforeClass, Test}


object WinnerServiceImplTest{

  private var conf: SparkConf = _
  private var sc: SparkContext = _

  @BeforeClass
  @throws[Exception]
  def setUpForClass(): Unit = {
    conf = new SparkConf().setAppName("Poker").setMaster("local[*]")
    sc = new SparkContext(conf)
  }
}

class WinnerServiceImplTest {

  private var winnerService: WinnerService = _

  @Before
  @throws[Exception]
  def setUp(): Unit = {
    winnerService = new WinnerServiceImpl()
  }

  @Test
  def getWinnerSparkName(): Unit = {
    val players = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy", 5)), Some(Bet("Vasiy", 5)))
    val spectators = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy", 5)), Some(Bet("Vasiy", 5)))
    val winner = winnerService.getWinnerNameSpark(WinnerServiceImplTest.sc.parallelize(players), WinnerServiceImplTest.sc.parallelize(spectators))

    Assert.assertEquals("Andrei", winner)
  }

  @Test
  def getWinnerSparkNameWinVasiy(): Unit = {
    val players = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy1", 51)), Some(Bet("Vasiy", 50)))
    val spectators = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy", 5)), Some(Bet("Vasiy", 5)))
    val winner = winnerService.getWinnerNameSpark(WinnerServiceImplTest.sc.parallelize(players), WinnerServiceImplTest.sc.parallelize(spectators))

    Assert.assertEquals("Vasiy1", winner)
  }

  @Test
  def getWinnerSparkNameWithNone(): Unit = {
    val players = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy1", 51)), None)
    val spectators = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy", 5)), Some(Bet("Vasiy", 5)))
    val winner = winnerService.getWinnerNameSpark(WinnerServiceImplTest.sc.parallelize(players), WinnerServiceImplTest.sc.parallelize(spectators))

    Assert.assertEquals("Vasiy1", winner)
  }

  @Test
  def getWinnerSparkNameWinSpectators(): Unit = {
    val players = List(None, None)
    val spectators = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy", 5)), Some(Bet("Vasiy", 5)))
    val winner = winnerService.getWinnerNameSpark(WinnerServiceImplTest.sc.parallelize(players), WinnerServiceImplTest.sc.parallelize(spectators))

    Assert.assertEquals("Andrei", winner)
  }



  @Test
  def getWinnerName(): Unit = {
    val players = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy", 5)), Some(Bet("Vasiy", 5)))
    val spectators = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy", 5)), Some(Bet("Vasiy", 5)))
    val winner = winnerService.getWinnerName(players, spectators)

    Assert.assertEquals("Andrei", winner)
  }

  @Test
  def getWinnerNameWinVasiy(): Unit = {
    val players = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy1", 51)), Some(Bet("Vasiy", 50)))
    val spectators = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy", 5)), Some(Bet("Vasiy", 5)))
    val winner = winnerService.getWinnerName(players, spectators)

    Assert.assertEquals("Vasiy1", winner)
  }

  @Test
  def getWinnerNameWithNone(): Unit = {
    val players = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy1", 51)), None)
    val spectators = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy", 5)), Some(Bet("Vasiy", 5)))
    val winner = winnerService.getWinnerName(players, spectators)

    Assert.assertEquals("Vasiy1", winner)
  }

  @Test
  def getWinnerNameWinSpectators(): Unit = {
    val players = List(None, None)
    val spectators = List(Some(Bet("Andrei", 10)), Some(Bet("Vasiy", 5)), Some(Bet("Vasiy", 5)))
    val winner = winnerService.getWinnerName(players, spectators)

    Assert.assertEquals("Andrei", winner)
  }
}
