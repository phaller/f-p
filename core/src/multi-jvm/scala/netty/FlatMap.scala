package netty

import scala.pickling._
import Defaults._
import shareNothing._

import scala.spores._
import SporePickler._

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

import silt.{SiloRef, Host, LocalSilo, Emitter, SiloSystem, SiloFactory}
import silt.netty.Server


object FlatMapMultiJvmNode1 {
  def main(args: Array[String]): Unit =
    Server(8090).run()
}

object FlatMapMultiJvmNode2 {
  val numPersons = 10

  def populateSilo(): LocalSilo[Person, List[Person]] = {
    val persons: List[Person] = for (_ <- (1 to numPersons).toList) yield {
      val (randomId, randomAge, randomLoc) = (Random.nextInt(10000000), Random.nextInt(100), Random.nextInt(200))
      new Person(randomId, randomAge, randomLoc)
    }
    new LocalSilo(persons)
  }

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000) // FIXME

    val system = SiloSystem()
    val host = Host("127.0.0.1", 8090)
    val siloFut1: Future[SiloRef[Int, List[Int]]] = system.fromClass[Int, List[Int]](classOf[TestSiloFactory], host)
    val siloFut2: Future[SiloRef[Int, List[Int]]] = system.fromClass[Int, List[Int]](classOf[TestSiloFactory], host)

    val siloFut3 = siloFut1.flatMap { (silo1: SiloRef[Int, List[Int]]) =>
      siloFut2.flatMap { (silo2: SiloRef[Int, List[Int]]) =>
        silo1.flatMap(spore {
          val localSilo2 = silo2
          (l1: List[Int]) =>
            localSilo2.apply[Int, List[Int]](spore {
              val localList = l1
              (l2: List[Int]) =>
                localList ++ l2
            })
        }).send()
      }

      //silo.send()
    }
    val res = Await.result(siloFut3, 5.seconds)
    assert(res.toString == "List(40, 30, 20)")

    system.waitUntilAllClosed()
  }

}
