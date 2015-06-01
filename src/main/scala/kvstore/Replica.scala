package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ask, pipe}
import scala.concurrent.duration._
import akka.util.Timeout

import scala.util.Random

object Replica {

    sealed trait Operation {
        def key: String

        def id: Long
    }

    case class Insert(key: String, value: String, id: Long) extends Operation

    case class Remove(key: String, id: Long) extends Operation

    case class Get(key: String, id: Long) extends Operation

    sealed trait OperationReply

    case class OperationAck(id: Long) extends OperationReply

    case class OperationFailed(id: Long) extends OperationReply

    case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

    def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

    import Replica._
    import Replicator._
    import Persistence._
    import context.dispatcher

    /*
     * The contents of this actor is just a suggestion, you can implement it in any way you like.
     */

    var expectedSeq: Long = 0L

    var kv = Map.empty[String, String]
    // a map from secondary replicas to replicators
    var secondaries = Map.empty[ActorRef, ActorRef]
    // the current set of replicators
    var replicators = Set.empty[ActorRef]

    var replicationSet = Set.empty[ActorRef]

    def persistence = context.system.actorOf(persistenceProps)

    override val supervisorStrategy = OneForOneStrategy(10) {
        case _: Exception =>
            SupervisorStrategy.restart
    }

    var opPesistedAckn: Map[Long, OpProcessindData] = Map.empty[Long, OpProcessindData]

    var seqToReplicator: Map[Long, ActorRef] = Map.empty[Long, ActorRef]

    var seqToSnapShot: Map[Long, Snapshot] = Map.empty[Long, Snapshot]

    var repAckn: Map[ActorRef, Long] = Map.empty[ActorRef, Long]

    var operationAckn: Map[Long, (Boolean, Boolean)] = Map.empty[Long, (Boolean, Boolean)]

    // register self in a Arbiter
    arbiter ! Join

    context.watch(self)

    def receive = {
        case JoinedPrimary => context.become(leader)
        case JoinedSecondary => context.become(replica)
    }

    /* TODO Behavior for  the leader role. */
    val leader: Receive = {
        case ins@Insert(key, value, id) => {
            opProcessing(id, key, Some(value), sender)
            replicators.foreach{replicator =>
                replicator ! Replicate(key, Some(value), id)
                repAckn +=  replicator -> id
            }
        }
        case rm@Remove(key, id) => {
            opProcessing(id, key, None, sender)
            replicators.foreach{replicator =>
                replicator ! Replicate(key, None, id)
                repAckn += replicator -> id
            }
        }
        case get@Get(key, id) => {
            sender ! GetResult(key, kv.get(key), id)
        }
        case Persisted(key, id) => {
            acknowledgement(id)
        }
        case Replicas(replicas) => {
            val newReplicas = replicas - self
            if(secondaries.isEmpty){
                replicate(newReplicas)
            }
            else
            {
                val currentReplicas = secondaries.toSet[(ActorRef, ActorRef)].map(_._1)
                val replicasToStop = currentReplicas -- newReplicas
                val replicatorsToStop = secondaries.filter(el => replicasToStop.contains(el._1)).toSet[(ActorRef, ActorRef)].map(_._2)
                replicatorsToStop.foreach{replicator =>
                    val id = repAckn.get(replicator)
                    repAckn -= replicator
                    replicators -= replicator
                    id.map(acknowledgement(_))
                    context.system.stop(replicator)
                }
                replicasToStop.foreach{replica =>
                    secondaries -= replica
                    replica ! PoisonPill
                }
                val replicasToReplicate = currentReplicas -- newReplicas
                replicate(replicasToReplicate)
            }

        }
        case Replicated(key, id) => {
            repAckn -= sender
            if(repAckn.isEmpty)
            opPesistedAckn.get(id).map { opProcessData =>
                opProcessData.opTimeOut.cancel()
                opProcessData.requester ! OperationAck(id)
                opPesistedAckn -= id
            }
        }
    }

    def replicate(replicas: Set[ActorRef]) =
    {
        replicas.foreach{replica =>
            val replicator = context.system.actorOf(Replicator.props(replica))
            secondaries += replica -> replicator
            replicators += replicator
            kv.foreach{case (k, v) =>
                val id = Random.nextLong
                replicator ! Replicate(k, Some(v), id)
                repAckn += replicator -> id
            }
        }
    }

    def acknowledgement(id: Long) = {
        opPesistedAckn.get(id).map{opProcessData =>
            opProcessData.persistOp.cancel()
            context.system.stop(opProcessData.persistActor)
            if(repAckn.find(el => el._2 == id).isEmpty)
            {
                opProcessData.opTimeOut.cancel()
                opPesistedAckn -= id
                opProcessData.requester ! OperationAck(id)
            }
        }
    }

    def opProcessing(id: Long, key: String, value: Option[String], requester: ActorRef) = {
        // persist block
        val persAct = persistence
        val persistOP = context.system.scheduler.schedule(0 millis, 100 millis){
            persAct ! Persist(key, value, id)
        }
        val opTimeOut = context.system.scheduler.scheduleOnce(1 second){
            persistOP.cancel()
            context.system.stop(persAct)
            requester ! OperationFailed(id)
            opPesistedAckn -= id
        }
        opPesistedAckn += id -> OpProcessindData(persistOP, opTimeOut, persAct, requester)

        // update kv block
        value match
        {
            case Some(v) => kv += key -> v
            case None => kv -= key
        }
    }
    case class OpProcessindData(persistOp: Cancellable, opTimeOut: Cancellable, persistActor: ActorRef, requester: ActorRef)

    /* TODO Behavior for the replica role. */
    val replica: Receive = {
        case get@Get(key, id) => {
            sender ! GetResult(key, kv.get(key), id)
        }
        case snap@Snapshot(key, valueOpt, seq) => {
            if (seq < expectedSeq)
                sender ! SnapshotAck(key, seq)
            else if (seq == expectedSeq && seqToReplicator.get(seq).isEmpty) {
                seqToReplicator += seq -> sender
                seqToSnapShot += seq -> snap
                opProcessing(seq, key, valueOpt, self)
            }
        }
        case Persisted(key, id) => {
            acknowledgement(id)
        }
        case OperationAck(id) => {
            seqToSnapShot.get(id).map{snap =>
                seqToReplicator.get(id).map{_ ! SnapshotAck(snap.key, id)}
                seqToSnapShot -= id
            }
            seqToReplicator -= id
            if (expectedSeq < id + 1) expectedSeq += 1
        }
        case OperationFailed(id) => {
            println(s"Secondary replica op: $id persistent failed!")
        }
        case _ => println("Unknown message")
    }

}

