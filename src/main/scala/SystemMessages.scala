package silt

import scala.spores.NullarySpore


////////////////////////////////
// commands to server

abstract class ReplyMessage {
  var id: Int = _
}

// commands that access a DS[T] should also pass along a ClassTag[T].
// that way we can cast the data to T before operating on it.
case class InitSilo(fqcn: String, refId: Int) extends ReplyMessage

case class InitSiloFun[U, T <: Traversable[U]](fun: NullarySpore[LocalSilo[U, T]], refId: Int) extends ReplyMessage

case class OKCreated(refId: Int) extends ReplyMessage

case class ApplyMessage[U, A <: Traversable[U], V, B <: Traversable[V]](refId: Int, fun: A => B, newRefId: Int)

case class ForceMessage(refId: Int) extends ReplyMessage

case class ForceResponse(value: Any) extends ReplyMessage

/**
 *  @tparam A old element type
 *  @tparam B new element type
 *  @tparam C new collection type
 */
case class PumpTo[A, B, C](emitterId: Int,
                           srcRefId: Int,
                           destRefId: Int,
                           destHost: Host,
                           fun: (A, Emitter[B]) => Unit,
                           bf: BuilderFactory[B, C])

case class CreatSilo(emitterId: Int, destRefId: Int, builder: AbstractBuilder)

case class Emit(emitterId: Int, destRefId: Int, ba: Any/*Array[Byte]*/)

case class Done(emitterId: Int, destRefId: Int)

case class Terminate()
