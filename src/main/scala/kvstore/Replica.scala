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
    val system = akka.actor.ActorSystem("system")

    // Internal Persist actor
    val persist = context.actorOf(persistenceProps)

    var kv = Map.empty[String, String]
    // a map from secondary replicas to replicators
    var secondaries = Map.empty[ActorRef, ActorRef]
    // the current set of replicators
    var replicators = Set.empty[ActorRef]
    // used to manage snapshots
    var expected = 0L
    // join an arbiter
    arbiter ! Join

    // hold cancellation tokens for persistence retry operation
    var cancelTokens = Map.empty[Long, Cancellable]

    // token for cancel  - send OperationFailed message within 1 second
    var cancelFailureToken = Map.empty[Long, Cancellable]

    // map from request id to client ActorRef
    var clients = Map.empty[Long, ActorRef]

    // map from key to request id
    var keyId = Map.empty[String, Long]

    // id generator
    var _id = 0L

    def nextId = {
        val ret = _id
        _id += 1
        ret
    }

    var _inValue = 0

    def nextVal = {
        val r = _inValue
        _inValue += 1
        r
    }

    var persisted = Set.empty[Long]

    var inReplication = Set.empty[ActorRef]
    //var inReplication = 0

    var idToPersisted = Map.empty[Long, Boolean]

    var internalReplicationIds = Set.empty[Long]

    var replicatorToUnacknowledgedIds = Map.empty[ActorRef, Set[Long]]

    var internalReplicators = Set.empty[ActorRef]

    var internalSecondaries = Map.empty[ActorRef, ActorRef]

    var idToAcknowledged = Set.empty[Long]

    var pendingSecondaries = Map.empty[ActorRef, ActorRef]


    override val supervisorStrategy = OneForOneStrategy() {
        case _ : PersistenceException => Restart
    }

    def receive = {
        case JoinedPrimary   => context.become(leader)
        case JoinedSecondary => context.become(replica)
    }

    context.watch(self)

    /* TODO Behavior for  the leader role. */
    val leader: Receive = {
        case Insert(key, value, id) =>
            // println("Insert  key:"+ key +" value: "+value + " id : "+id + " number of replicas: "+replicators.size)
            // increment number of needed replications

            clients += id -> sender
            keyId += key -> id
            kv += key -> value

            idToPersisted += id -> false
            idToAcknowledged += id

            cancelTokens += ((id,system.scheduler.schedule(0 millis, 100 millis, persist, Persist(key, Some(value), id))))

            replicators.foreach{replicator =>
                replicator ! Replicate(key, Some(value), id)
                inReplication += replicator
                replicatorToUnacknowledgedIds += replicator -> Set.empty[Long]
                replicatorToUnacknowledgedIds += replicator -> (replicatorToUnacknowledgedIds(replicator)+id)
            }
            cancelFailureToken += (( id, system.scheduler.scheduleOnce(1.seconds){
                clients(id) ! OperationFailed(id)
            }))
        //context.sender ! OperationAck(id)
        case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

        case Remove(key, id) =>
            //println("Remove  key:"+ key + " id : "+id + "number of replicas: "+replicators.size)
            clients += ((id,sender))
            keyId += key -> id
            kv= kv -(key)
            idToAcknowledged += id
            idToPersisted += id->false
            cancelTokens += ((id,system.scheduler.schedule(0 millis,100 millis,persist,Persist(key,None,id))))

            cancelFailureToken += (( id,system.scheduler.scheduleOnce(1.seconds){
                clients(id) ! OperationFailed(id)
            }))

            // replicate changes to all replicas
            replicators.foreach{f=>
                f! Replicate(key,None,id)
                inReplication += f
                replicatorToUnacknowledgedIds += f->Set.empty[Long]
                replicatorToUnacknowledgedIds += f->(replicatorToUnacknowledgedIds(f)+id)
            }

        //context.sender ! OperationAck(id)
        case Replicas(replicas) =>
            var newSecondaries = Set.empty[ActorRef]
            secondaries.foreach(p=> newSecondaries += p._1)
            if(replicas.filter(p=>p!=self) != newSecondaries){
                println("ooo")
                internalReplicationIds = Set()
                replicas.filter(p=>p!=self).foreach(replica=>{
                    if(!secondaries.contains(replica)){
                        var rep = context.actorOf(Replicator.props(replica))
                        replicatorToUnacknowledgedIds += rep->Set.empty[Long]
                        //var inRep = context.actorOf(InternalReplicator.props(replica))
                        replicators += rep
                        //internalReplicators += inRep
                        pendingSecondaries += rep->replica
                        secondaries += replica -> rep
                        // internalSecondaries += replica -> inRep
                        kv.foreach(pair=>{
                            rep ! Replicate(pair._1,Some(pair._2),keyId(pair._1))
                            // internalReplicationIds += keyId(pair._1)
                        })
                    }
                })
                secondaries.foreach(pair =>{
                    if(!replicas.contains(pair._1)){
                        replicators -= pair._2
                        //context.stop(pair._2)
                        pair._2 ! PoisonPill
                        secondaries -= pair._1
                        // ??????
                        //context.stop(pair._1)
                        inReplication -= pair._2
                        if(replicatorToUnacknowledgedIds.contains(pair._2))
                            replicatorToUnacknowledgedIds(pair._2).foreach(id=>{
                                if(cancelFailureToken.contains(id))
                                    cancelFailureToken(id).cancel()
                                if(idToPersisted.contains(id)&&idToPersisted(id)){
                                    clients(id) ! OperationAck(id)
                                }
                            })

                    }
                })
            }

        case Replicated(key, id) =>
            internalReplicationIds  -= id
            //
            if(replicatorToUnacknowledgedIds.contains(sender))
                replicatorToUnacknowledgedIds += sender -> (replicatorToUnacknowledgedIds(sender)-id)
            if(idToPersisted.contains(id))
                if(idToPersisted(id) && replicators.forall(r=> !replicatorToUnacknowledgedIds(r).contains(id))){
                    if(idToAcknowledged.contains(id)){
                        cancelFailureToken(id).cancel()
                        if(clients.contains(id))
                            clients(id) ! OperationAck(id)
                        idToAcknowledged -= id
                    }
                }

        case Persisted(key,id)=>
            // println("From leader Persisted id: " + id)

            idToPersisted += id->true
            persisted += id
            if(cancelTokens.contains(id))
                cancelTokens(id).cancel()
            //if(inReplication.isEmpty){
            if(replicators.forall(r=> !replicatorToUnacknowledgedIds(r).contains(id))){
                cancelFailureToken(id).cancel()
                idToAcknowledged -= id
                if(clients.contains(id))
                    clients(id) ! OperationAck(id)
            }
        case _ => println("Unexpected message")
    }

    /* TODO Behavior for the replica role. */
    val replica: Receive = {
        case Get(key,id) => sender ! GetResult(key, kv.get(key), id)

        case Snapshot(key, valueOption, seq) =>
            // println("From replica recived snapshot  seq =" + seq)
            secondaries += ((self, sender))
            if(seq<expected)
                sender ! SnapshotAck(key, seq)
            else if(seq == expected){
                valueOption match {
                    case Some(value) =>
                        kv += ((key, value))
                        //create local persistence actor

                        //repeat persistence until succeed
                        // println("From Secondary replica persistence seq = "+ seq)
                        cancelTokens += ((seq, system.scheduler.schedule(0 millis, 100 millis, persist, Persist(key,valueOption,seq))))
                    //persist ! Persist(key,valueOption,seq)
                    //sender ! SnapshotAck(key, seq)
                    case None => kv -= (key);
                        //sender ! SnapshotAck(key, seq)
                        cancelTokens += ((seq,system.scheduler.schedule(0 millis, 100 millis, persist, Persist(key,valueOption,seq))))
                }
                expected = seq+1
            }
            else //if(seq > expected)
                println("we do nothing here")
        //       else
        //         expected += 1
        case Persisted(key,id)=>
            if(cancelTokens.contains(id))
                cancelTokens(id).cancel()
            if(secondaries.contains(self))
                secondaries(self) ! SnapshotAck(key, id)
        case _ =>
    }

}

