package kvstore

import akka.actor.{Cancellable, Props, Actor, ActorRef}
import scala.concurrent.duration._

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._
  import Replica._
  import context.dispatcher

    /*
     * The contents of this actor is just a suggestion, you can implement it in any way you like.
     */

    // map from sequence number to pair of sender and request
    var acks = Map.empty[Long, (ActorRef, Replicate)]
    // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
    var pending = Vector.empty[Snapshot]

    var cancelTokens = Map.empty[Long,Cancellable]
    var _seqCounter = 0L
    def nextSeq = {
        val ret = _seqCounter
        _seqCounter += 1
        ret
    }

    /* TODO Behavior for the Replicator. */
    def receive: Receive = {
        case rep@Replicate(key, valueOption, id) =>
            acks += ((_seqCounter,(sender, rep )))
            replica ! Snapshot(key,valueOption,_seqCounter)
            cancelTokens += ((_seqCounter, context.system.scheduler.schedule(30 millis, 80 millis,replica, Snapshot(key,valueOption,_seqCounter))))
            nextSeq
        // replica ! Snapshot(key,valueOption,nextSeq)
        case SnapshotAck(key, seq) =>
            //println("seq: "+seq +" the rest :"+acks(seq))
            acks.get(seq) match {
                case Some((s, act)) =>
                    //println("From replicator, replicated id :" + act.id + " seq. number: "+ seq)
                    s!Replicated(act.key,act.id)
                    cancelTokens.get(seq) match {
                        case Some(c)=>
                            c.cancel()
                        //cancelTokens -= (seq)
                        case _ =>
                    }
                // acks -= (seq)
                case _ =>  println("Unexpected case in Replicator")
            }
        //acks -= (seq)
        //cancelTokens(seq).cancel()
        //cancelTokens -= (seq)

        case _ =>
    }

}
