package com.github.tarao
package collection
package relation

import scala.collection.{breakOut, IterableLike}
import scala.collection.generic.CanBuildFrom
import scala.language.{higherKinds, implicitConversions}

/** A class to describe a join of two sequences. */
case class Join[A, B, K, It[X] <: IterableLike[X, It[X]]](
  left: It[A],
  right: Iterable[B],
  leftKey: A => K,
  rightKey: B => K,
  default: K => Option[B]
) {
  private type R = (A, B)

  private def applyTo[It2[X] <: IterableLike[X, It2[X]], R2, That](
    f: R => R2,
    s: It2[A]
  )(implicit bf: CanBuildFrom[It2[A], R2, That]): That = {
    val m = {
      val m: Map[K, Option[B]] =
        right.map(x => rightKey(x) -> Some(x))(breakOut)
      m.withDefault(default)
    }
    s.flatMap(x => m(leftKey(x)).map(y => f((x, y))))
  }

  def into[R2](f: R => R2)(implicit
    bf: CanBuildFrom[It[A], R2, It[R2]]
  ): It[R2] = applyTo(f, left)

  def result(implicit bf: CanBuildFrom[It[A], R, It[R]]): It[R] =
    applyTo(identity, left)

  def toStream: Stream[R] = applyTo(identity, left.toStream)(breakOut)
}
object Join {
  class Inner[A, B, It[X] <: IterableLike[X, It[X]]](
    left: It[A],
    right: Iterable[B]
  ) {
    def on[K](
      leftKey: A => K,
      rightKey: B => K
    ): Join[A, B, K, It] = Join(left, right, leftKey, rightKey, _ => None)
  }

  class Left[A, B, It[X] <: IterableLike[X, It[X]]](
    left: It[A],
    right: Iterable[B]
  ) {
    def on[K](
      leftKey: A => K,
      rightKey: B => K
    ): Left.On[A, B, K, It] = Left.On(left, right, leftKey, rightKey)
  }
  object Left {
    case class On[A, B, K, It[X] <: IterableLike[X, It[X]]](
      left: It[A],
      right: Iterable[B],
      leftKey: A => K,
      rightKey: B => K
    ) {
      private def toJoin(default: K => Option[B]): Join[A, B, K, It] =
        Join(left, right, leftKey, rightKey, default)

      def apply(default: B): Join[A, B, K, It] =
        toJoin(_ => Some(default))

      def apply(default: K => B): Join[A, B, K, It] =
        toJoin(x => Some(default(x)))
    }
  }

  implicit def toStream[A, B, K, It[X] <: IterableLike[X, It[X]], Col](
    join: Join[A, B, K, It]
  ): Stream[(A, B)] = join.toStream
}
