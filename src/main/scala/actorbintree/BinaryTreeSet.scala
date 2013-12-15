/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue
import actorbintree.BinaryTreeNode.{CopyFinished, CopyTo}

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with Stash {

  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case operation: Operation => root forward operation
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
    case _ => ???
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC => // ignore
    case operation: Operation => {
      stash()
    }
    case CopyFinished =>
      context become normal
      root ! PoisonPill
      root = newRoot
      unstashAll()
  }

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case op@Contains(requester, id, e) if e < elem && subtrees.contains(Left) => subtrees(Left) forward op
    case op@Contains(requester, id, e) if e > elem && subtrees.contains(Right) => subtrees(Right) forward op
    case Contains(requester, id, e) => requester ! ContainsResult(id, result = e == elem && !removed)
    case insert@Insert(requester, id, e) => {
      if (e < elem) forward(Left, insert)
      if (e > elem) forward(Right, insert)
      if (e == elem) {
        removed = false
        requester ! OperationFinished(id)
      }
    }
    case op@Remove(requester, id, e) =>
      if (e == elem) {
        removed = true
        requester ! OperationFinished(id)
      } else subtree(e) match {
        case Some(node) => node forward op
        case None => requester ! OperationFinished(id)
      }

    case msg@CopyTo(root) =>
      if (!removed) root ! Insert(self, elem, elem)
      val nodes = subtrees.values.toSet
      nodes.foreach(_ ! msg)
      context.become(copyingNext(nodes, insertConfirmed = removed))

    case _ => ???
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(`elem`) => context become copyingNext(expected, insertConfirmed = true)
    case CopyFinished => context become copyingNext(expected - sender, insertConfirmed)
  }

  private def copyingNext(expected: Set[ActorRef], insertConfirmed: Boolean): Receive =
    if (expected == Set.empty && insertConfirmed) {
      context.parent ! CopyFinished
      normal
    }
    else copying(expected, insertConfirmed)

  private def subtree(e: Int) = {
    if (e < elem) subtrees.get(Left)
    else if (e > elem) subtrees.get(Right)
    else None
  }

  private def forward(position: Position, insert: Insert) {
    subtrees.get(position) match {
      case Some(node) => node forward insert
      case None =>
        val node = context.actorOf(props(insert.elem, initiallyRemoved = false))
        subtrees = subtrees updated(position, node)
        insert.requester ! OperationFinished(insert.id)
    }
  }
}
