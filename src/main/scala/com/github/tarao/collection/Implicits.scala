package com.github.tarao
package collection

import collection.relation.Join
import scala.language.higherKinds

object Implicits {
  import scala.language.implicitConversions

  implicit def iterableOps[ItLike, A, It[X] <: IterableLike[X, It, It[X]]](
    it: ItLike
  )(implicit toIt: ItLike => It[A]): IterableOps[A, It] =
    new IterableOps(toIt(it))

  class IterableOps[A, It[X] <: IterableLike[X, It, It[X]]](
    override protected val it: It[A]
  ) extends AnyVal with collection.IterableOps[A, It]
}
