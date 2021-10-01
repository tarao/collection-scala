package com.github.tarao

package object collection {
  type IterableLike[+A, +CC[_], +C] = scala.collection.IterableOps[A, CC, C]
  type BuildFrom[-From, -A, +C] = scala.collection.BuildFrom[From, A, C]

  private[collection] implicit class IterableCompatibility[A, It[X] <: IterableLike[X, It, It[X]]](
    private val it: It[A]
  ) extends AnyVal {
    def convertTo[C1](factory: scala.collection.Factory[A, C1]): C1 = it.to(factory)
  }
}
