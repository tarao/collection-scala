package com.github.tarao

import scala.collection.GenTraversable
import scala.collection.generic.GenericCompanion
import scala.language.higherKinds

package object collection {
  type IterableLike[+A, +CC[_], +C] = scala.collection.IterableLike[A, C]

  type LazyList[+A] = Stream[A]
  val LazyList = Stream

  type CanBuildFrom[-From, -A, +C] = scala.collection.generic.CanBuildFrom[From, A, C]

  private[collection] implicit class IterableCompatibility[A, It[X] <: IterableLike[X, It, It[X]]](
    private val it: It[A]
  ) extends AnyVal {
    def convertTo[F, Col[X] <: GenTraversable[X]](factory: GenericCompanion[Col])(implicit
      cbf: CanBuildFrom[Nothing, A, Col[A]]
    ): Col[A] = it.to[Col]
  }
}
