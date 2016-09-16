package silt
package graph

import scala.language.existentials
import scala.reflect.runtime

import scala.pickling._
import Defaults._
import shareNothing._
import binary._

import scala.spores.{Spore, Spore2}

import scala.concurrent.util.Unsafe


object Picklers {

  implicit def doPumpToPickler[A: FastTypeTag, B: FastTypeTag, P: FastTypeTag]: Pickler[DoPumpTo[A, B, P]] with Unpickler[DoPumpTo[A, B, P]] =
    new Pickler[DoPumpTo[A, B, P]] with Unpickler[DoPumpTo[A, B, P]] {
      def tag: FastTypeTag[DoPumpTo[A, B, P]] = FastTypeTag[DoPumpTo[A, B, P]]

      def pickle(picklee: DoPumpTo[A, B, P], builder: PBuilder): Unit = {
        builder.beginEntry(picklee, tag)

        builder.putField("node", { b =>
          val node = picklee.node
          val tag = node match {
            case m: Materialized =>
              implicitly[FastTypeTag[Materialized]]
            case a: Apply[u, t, v, s] =>
              // need to pickle the erased type in this case
              FastTypeTag.makeRaw(a.getClass).asInstanceOf[FastTypeTag[Any]]
            case mi: MultiInput[r] =>
              // need to pickle the erased type in this case
              FastTypeTag.makeRaw(mi.getClass).asInstanceOf[FastTypeTag[Any]]
          }
          NodePU.pickle(node, b)
        })

        builder.putField("fun", { b =>
          // pickle spore
          val newBuilder = pickleFormat.createBuilder()
          println(s"picklee.pickler: ${picklee.pickler}")
          println(s"picklee.fun: ${picklee.fun}")
          println(s"!!! picklee.unpickler.getClass.getName: ${picklee.unpickler.getClass.getName}")
          picklee.pickler.asInstanceOf[Pickler[Any]].pickle(picklee.fun, newBuilder)
          val p = newBuilder.result()
          val sd = SelfDescribing(picklee.unpickler.getClass.getName, p.value)
          sd.pickleInto(b)
        })

        builder.putField("pickler", { b =>
          scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.pickler.getClass.getName, b)
        })

        builder.putField("unpickler", { b =>
          scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.unpickler.getClass.getName, b)
        })

        builder.putField("emitterId", { b =>
          val intPickler = pickler.AllPicklers.intPickler
          intPickler.pickle(picklee.emitterId, b)
        })
        builder.putField("destHost", { b =>
          val hostPickler = implicitly[Pickler[Host]]
          hostPickler.pickle(picklee.destHost, b)
        })
        builder.putField("destRefId", { b =>
          val intPickler = pickler.AllPicklers.intPickler
          intPickler.pickle(picklee.destRefId, b)
        })
        builder.endEntry()
      }

      def unpickle(tag: String, reader: PReader): Any = {
        val reader1 = reader.readField("node")
        val tag1 = reader1.beginEntry()
        val node = NodePU.unpickle(tag1, reader1).asInstanceOf[Node]
        reader1.endEntry()

        val reader3 = reader.readField("fun")
        val typestring3 = reader3.beginEntry()
        val unpickler3 = implicitly[Unpickler[SelfDescribing]]
        val sd = unpickler3.unpickle(typestring3, reader3).asInstanceOf[SelfDescribing]
        val fun = sd.result().asInstanceOf[Spore2[A, Emitter[B], Unit]]
        reader3.endEntry()

        val reader4 = reader.readField("pickler")
        val tag4 = reader4.beginEntry()
        val picklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag4, reader4).asInstanceOf[String]
        reader4.endEntry()
        val pickler = Unsafe.instance.allocateInstance(Class.forName(picklerClassName)).asInstanceOf[Pickler[Spore2[A, Emitter[B], Unit]]]

        val reader5 = reader.readField("unpickler")
        val tag5 = reader5.beginEntry()
        val unpicklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag5, reader5).asInstanceOf[String]
        reader5.endEntry()
        val unpickler = Unsafe.instance.allocateInstance(Class.forName(unpicklerClassName)).asInstanceOf[Unpickler[Spore2[A, Emitter[B], Unit]]]

        val reader6 = reader.readField("emitterId")
        val tag6 = reader6.beginEntry()
        val emitterId = scala.pickling.pickler.AllPicklers.intPickler.unpickle(tag6, reader6).asInstanceOf[Int]
        reader6.endEntry()
        val reader7 = reader.readField("destHost")
        val tag7 = reader7.beginEntry()
        val destHost = implicitly[Unpickler[Host]].unpickle(tag7, reader7).asInstanceOf[Host]
        reader7.endEntry()
        val reader8 = reader.readField("destRefId")
        val tag8 = reader8.beginEntry()
        val destRefId = scala.pickling.pickler.AllPicklers.intPickler.unpickle(tag8, reader8).asInstanceOf[Int]
        reader8.endEntry()

        DoPumpTo[A, B, Spore2[A, Emitter[B], Unit]](node, fun, pickler, unpickler, emitterId, destHost, destRefId)
      }
    }

  implicit object GraphPU extends Pickler[Graph] with Unpickler[Graph] {
    def tag: FastTypeTag[Graph] = implicitly[FastTypeTag[Graph]]

    def pickle(picklee: Graph, builder: PBuilder): Unit = {
      builder.beginEntry(picklee, tag)
      builder.putField("node", { b =>
        val node = picklee.node
        val tag = node match {
          case m: Materialized =>
            implicitly[FastTypeTag[Materialized]]
          case a: Apply[u, t, v, s] =>
            // need to pickle the erased type in this case
            FastTypeTag.makeRaw(a.getClass).asInstanceOf[FastTypeTag[Any]]
          case mi: MultiInput[r] =>
            // need to pickle the erased type in this case
            FastTypeTag.makeRaw(mi.getClass).asInstanceOf[FastTypeTag[Any]]
        }
        NodePU.pickle(node, b)
      })
      builder.endEntry()
    }

    def unpickle(tag: String, reader: PReader): Any = {
      // val nodeTag = implicitly[FastTypeTag[Node]]
      val reader1 = reader.readField("node")
      // reader1.hintTag(nodeTag)
      val tag1 = reader1.beginEntry()
      println(s"GraphPU: unpickle, tag: $tag, tag1: $tag1")
      val node = NodePU.unpickle(tag1, reader1).asInstanceOf[Node]
      reader1.endEntry()
      Graph(node)
    }
  }

  implicit object CommandEnvelopePU extends Pickler[CommandEnvelope] with Unpickler[CommandEnvelope] {
    def tag: FastTypeTag[CommandEnvelope] = implicitly[FastTypeTag[CommandEnvelope]]
    def pickle(picklee: CommandEnvelope, builder: PBuilder): Unit = {
      builder.beginEntry(picklee, tag)
      builder.putField("cmd", { b =>
        val cmd = picklee.cmd
        val tag = cmd match {
          case d: DoPumpTo[a, b, p] =>
            // need to pickle the erased type in this case
            FastTypeTag.makeRaw(d.getClass).asInstanceOf[FastTypeTag[Any]]
        }
        CommandPU.pickle(cmd, b)
      })
      builder.endEntry()
    }
    def unpickle(tag: String, reader: PReader): Any = {
      val reader1 = reader.readField("cmd")
      val tag1 = reader1.beginEntry()
      println(s"CommandEnvelopePU: unpickle, tag: $tag, tag1: $tag1")
      val cmd = CommandPU.unpickle(tag1, reader1).asInstanceOf[Command]
      reader1.endEntry()
      CommandEnvelope(cmd)
    }
  }

  implicit object CommandPU extends Pickler[Command] with Unpickler[Command] {
    def tag = implicitly[FastTypeTag[Command]]
    def pickle(picklee: Command, builder: PBuilder): Unit = picklee match {
      case d: DoPumpTo[a, b, p] =>
        doPumpToPickler[a, b, p].pickle(d, builder)
    }
    def unpickle(tag: String, reader: PReader): Any = {
      println(s"CommandPU.unpickle, tag: $tag")
      doPumpToPickler[Any, Any, Any].unpickle(tag, reader)
    }
  }

  implicit object NodePU extends Pickler[Node] with Unpickler[Node] {
    def tag: FastTypeTag[Node] = implicitly[FastTypeTag[Node]]

    def pickle(picklee: Node, builder: PBuilder): Unit = picklee match {
      case m: Materialized =>
        implicitly[Pickler[Materialized]].pickle(m, builder)
      case a: Apply[u, t, v, s] =>
        applyPU[u, t, v, s].pickle(a, builder)
      case mi: MultiInput[r] =>
        multiInputPU[r].pickle(mi, builder)
    }

    def unpickle(tag: String, reader: PReader): Any = {
      println(s"NodePU.unpickle, tag: $tag")
      if (tag.startsWith("silt.graph.MultiInput")) {
        multiInputPU[Any].unpickle(tag, reader)
      } else if (tag.startsWith("silt.graph.Materialized")) {
        implicitly[Unpickler[Materialized]].unpickle(tag, reader)
      } else { // no other cases possible because of `sealed`
        applyPU[Any, Traversable[Any], Any, Traversable[Any]].unpickle(tag, reader)
      }
    }
  }

  implicit def multiInputPU[R: FastTypeTag]: Pickler[MultiInput[R]] with Unpickler[MultiInput[R]] =
    new Pickler[MultiInput[R]] with Unpickler[MultiInput[R]] {

    def tag: FastTypeTag[MultiInput[R]] = FastTypeTag[MultiInput[R]]

    def pickle(picklee: MultiInput[R], builder: PBuilder): Unit = {
      builder.beginEntry(picklee, tag)

      val inputsPickler = scala.pickling.pickler.AllPicklers.seqPickler[PumpNodeInput[Any, Any, R, Any]]
      val inputsTag = FastTypeTag[Seq[PumpNodeInput[Any, Any, R, Any]]]

      builder.putField("inputs", { b =>
        inputsPickler.pickle(picklee.inputs.asInstanceOf[Seq[PumpNodeInput[Any, Any, R, Any]]], b)
      })

      builder.putField("refId", { b =>
        scala.pickling.pickler.AllPicklers.intPickler.pickle(picklee.refId, b)
      })

      builder.putField("destHost", { b =>
        val hostPickler = implicitly[Pickler[Host]]
        hostPickler.pickle(picklee.destHost, b)
      })

      builder.putField("emitterId", { b =>
        scala.pickling.pickler.AllPicklers.intPickler.pickle(picklee.emitterId, b)
      })

      builder.endEntry()
    }

    def unpickle(tag: String, reader: PReader): Any = {
      // println(s"multiInputPU.unpickle, tag: $tag")
      val inputsUnpickler = scala.pickling.pickler.AllPicklers.seqPickler[PumpNodeInput[Any, Any, R, Any]]
      val inputsTag = implicitly[FastTypeTag[Seq[PumpNodeInput[Any, Any, R, Any]]]]

      val reader1 = reader.readField("inputs")
      val tag1 = reader1.beginEntry()
      val inputs = inputsUnpickler.unpickle(tag1, reader1).asInstanceOf[Seq[PumpNodeInput[_, _, R, _]]]
      reader1.endEntry()

      val reader2 = reader.readField("refId")
      val tag2 = reader2.beginEntry()
      val refId = pickler.AllPicklers.intPickler.unpickle(tag2, reader2).asInstanceOf[Int]
      reader2.endEntry()

      val reader3 = reader.readField("destHost")
      val hostUnpickler = implicitly[Unpickler[Host]]
      val tag3 = reader3.beginEntry()
      val destHost = hostUnpickler.unpickle(tag3, reader3).asInstanceOf[Host]
      reader3.endEntry()

      val reader4 = reader.readField("emitterId")
      val tag4 = reader4.beginEntry()
      val emitterId = pickler.AllPicklers.intPickler.unpickle(tag4, reader4).asInstanceOf[Int]
      reader4.endEntry()

      MultiInput[R](inputs, refId, destHost, emitterId)
    }
  }

  implicit def pumpNodeInputPU[U: FastTypeTag, V: FastTypeTag, R: FastTypeTag, P: FastTypeTag]: Pickler[PumpNodeInput[U, V, R, P]] with Unpickler[PumpNodeInput[U, V, R, P]] =
    new Pickler[PumpNodeInput[U, V, R, P]] with Unpickler[PumpNodeInput[U, V, R, P]] {

    def tag: FastTypeTag[PumpNodeInput[U,V,R,P]] = FastTypeTag[PumpNodeInput[U,V,R,P]]

    def pickle(picklee: PumpNodeInput[U, V, R, P], builder: PBuilder): Unit = {
      builder.beginEntry(picklee, tag)

      builder.putField("from", { b =>
        picklee.from match {
          case m: Materialized =>
            implicitly[Pickler[Materialized]].pickle(m, b)
          case a: Apply[u, t, v, s] =>
            // need to pickle the erased type in this case
            val clazz = a.getClass
            val tag = FastTypeTag.makeRaw(clazz).asInstanceOf[FastTypeTag[Any]]
            applyPU[u, t, v, s].pickle(a, b)
          case _ => ???
        }
      })

      builder.putField("fromHost", { b =>
        val hostPickler = implicitly[Pickler[Host]]
        hostPickler.pickle(picklee.fromHost, b)
      })

      builder.putField("fun", { b =>
        // pickle spore
        val newBuilder = pickleFormat.createBuilder()
        picklee.pickler.asInstanceOf[Pickler[Any]].pickle(picklee.fun, newBuilder)
        val p = newBuilder.result()
        val sd = SelfDescribing(picklee.unpickler.getClass.getName, p.value)
        sd.pickleInto(b)
      })

      builder.putField("pickler", { b =>
        scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.pickler.getClass.getName, b)
      })

      builder.putField("unpickler", { b =>
        scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.unpickler.getClass.getName, b)
      })

      builder.putField("bf", { b =>
        val clazz = picklee.bf.getClass
        println(s"class of fun: ${clazz.getName}")
        val tag = FastTypeTag.makeRaw(clazz).asInstanceOf[FastTypeTag[Any]]
        //val pickler = scala.pickling.runtime.RuntimePicklerLookup.genPickler(clazz.getClassLoader, clazz, tag).asInstanceOf[Pickler[Any]]
        // new approach:
        // acquire scala.pickling.internal.GRL lock
        //internal.GRL.lock()
        //val rt = new scala.pickling.runtime.RuntimePickler(clazz.getClassLoader, clazz, tag)
        //val pickler = rt.mkPickler.asInstanceOf[Pickler[Any]]
        val pickler = scala.pickling.internal.currentRuntime.picklers.genPickler(clazz.getClassLoader, clazz, tag).asInstanceOf[Pickler[Any]]
        pickler.pickle(picklee.bf, b)
        //internal.GRL.unlock()
      })

      builder.endEntry()
    }

    def unpickle(tag: String, reader: PReader): Any = {
      val reader1 = reader.readField("from")
      val tag1 = reader1.beginEntry()
      // println(s"pumpNodeInputPU: unpickle, tag: $tag, tag1: $tag1")
      val from = NodePU.unpickle(tag1, reader1).asInstanceOf[Node]
      reader1.endEntry()

      val reader2 = reader.readField("fromHost")
      val hostUnpickler = implicitly[Unpickler[Host]]
      val tag2 = reader2.beginEntry()
      val fromHost = hostUnpickler.unpickle(tag2, reader2).asInstanceOf[Host]
      reader2.endEntry()

      val reader3 = reader.readField("fun")
      val typestring3 = reader3.beginEntry()
      val unpickler3 = implicitly[Unpickler[SelfDescribing]]
      val sd = unpickler3.unpickle(typestring3, reader3).asInstanceOf[SelfDescribing]
      val fun = sd.result().asInstanceOf[Spore2[U, Emitter[V], Unit]]
      reader3.endEntry()

      val reader4 = reader.readField("pickler")
      val tag4 = reader4.beginEntry()
      val picklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag4, reader4).asInstanceOf[String]
      reader4.endEntry()
      val pickler = Unsafe.instance.allocateInstance(Class.forName(picklerClassName)).asInstanceOf[Pickler[Spore2[U, Emitter[V], Unit]]]

      val reader5 = reader.readField("unpickler")
      val tag5 = reader5.beginEntry()
      val unpicklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag5, reader5).asInstanceOf[String]
      reader5.endEntry()
      val unpickler = Unsafe.instance.allocateInstance(Class.forName(unpicklerClassName)).asInstanceOf[Unpickler[Spore2[U, Emitter[V], Unit]]]

      val reader6 = reader.readField("bf")
      val tag6 = reader6.beginEntry()
      //val unpickler6 = scala.pickling.runtime.RuntimeUnpicklerLookup.genUnpickler(runtime.currentMirror, tag6)
      val unpickler6 = scala.pickling.internal.currentRuntime.picklers.genUnpickler(runtime.currentMirror, tag6)
      val bf = unpickler6.unpickle(tag6, reader6).asInstanceOf[BuilderFactory[V, R]]
      reader6.endEntry()

      PumpNodeInput[U, V, R, Spore2[U, Emitter[V], Unit]](from, fromHost, fun, pickler, unpickler, bf)
    }
  }

  implicit def applyPU[U: FastTypeTag, T <: Traversable[U] : FastTypeTag, V: FastTypeTag, S <: Traversable[V] : FastTypeTag]:
    Pickler[Apply[U, T, V, S]] with Unpickler[Apply[U, T, V, S]] = new Pickler[Apply[U, T, V, S]] with Unpickler[Apply[U, T, V, S]] {

    def tag: FastTypeTag[Apply[U,T,V,S]] = FastTypeTag[Apply[U,T,V,S]]

    def pickle(picklee: Apply[U, T, V, S], builder: PBuilder): Unit = {
      // println(s"applyPU: pickling $picklee")
      builder.beginEntry(picklee, tag)

      builder.putField("input", { b =>
        picklee.input match {
          case m: Materialized =>
            implicitly[Pickler[Materialized]].pickle(m, b)
          case a: Apply[u, t, v, s] =>
            // need to pickle the erased type in this case
            val clazz = a.getClass
            val tag = FastTypeTag.makeRaw(clazz).asInstanceOf[FastTypeTag[Any]]
            applyPU[u, t, v, s].pickle(a, b)
          case mi: MultiInput[r] =>
            // need to pickle the erased type in this case
            val clazz = mi.getClass
            val tag = FastTypeTag.makeRaw(clazz).asInstanceOf[FastTypeTag[Any]]
            multiInputPU[r].pickle(mi, b)            
        }
      })

      builder.putField("refId", { b =>
        scala.pickling.pickler.AllPicklers.intPickler.pickle(picklee.refId, b)
      })

      builder.putField("fun", { b =>
        // pickle spore
        val newBuilder = pickleFormat.createBuilder()
        picklee.pickler.asInstanceOf[Pickler[Any]].pickle(picklee.fun, newBuilder)
        val p = newBuilder.result()
        val sd = SelfDescribing(picklee.unpickler.getClass.getName, p.value)
        sd.pickleInto(b)
      })

      builder.putField("pickler", { b =>
        scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.pickler.getClass.getName, b)
      })

      builder.putField("unpickler", { b =>
        scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.unpickler.getClass.getName, b)
      })

      builder.endEntry()
    }

    def unpickle(tag: String, reader: PReader): Any = {
      val reader1 = reader.readField("input")
      val tag1 = reader1.beginEntry()
      val typeString = tag1
      println(s"applyPU typeString: $typeString")
      val input = if (typeString.startsWith("silt.graph.MultiInput")) {
        multiInputPU[Any].unpickle(tag1, reader1)
      } else if (typeString.startsWith("silt.graph.Materialized")) {
        implicitly[Unpickler[Materialized]].unpickle(tag1, reader1)
      } else { // no other cases possible because of `sealed`
        // throw new Exception(s"applyPU.unpickle, typeString: $typeString")
        applyPU[Any, Traversable[Any], Any, Traversable[Any]].unpickle(tag1, reader1)
      }
      reader1.endEntry()

      val reader2 = reader.readField("refId")
      val tag2 = reader2.beginEntry()
      val refId = scala.pickling.pickler.AllPicklers.intPickler.unpickle(tag2, reader2)
      reader2.endEntry()

      val reader3 = reader.readField("fun")
      val typestring3 = reader3.beginEntry()
      val unpickler3 = implicitly[Unpickler[SelfDescribing]]
      val sd = unpickler3.unpickle(typestring3, reader3).asInstanceOf[SelfDescribing]
      val fun = sd.result()
      reader3.endEntry()

      val reader4 = reader.readField("pickler")
      val tag4 = reader4.beginEntry()
      val picklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag4, reader4).asInstanceOf[String]
      reader4.endEntry()
      val pickler = Unsafe.instance.allocateInstance(Class.forName(picklerClassName)).asInstanceOf[Pickler[Spore[T, S]]]

      val reader5 = reader.readField("unpickler")
      val tag5 = reader5.beginEntry()
      val unpicklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag5, reader5).asInstanceOf[String]
      reader5.endEntry()
      val unpickler = Unsafe.instance.allocateInstance(Class.forName(unpicklerClassName)).asInstanceOf[Unpickler[Spore[T, S]]]

      Apply[U, T, V, S](input.asInstanceOf[Node], refId.asInstanceOf[Int], fun.asInstanceOf[T => S], pickler, unpickler)
    }
  }

}
