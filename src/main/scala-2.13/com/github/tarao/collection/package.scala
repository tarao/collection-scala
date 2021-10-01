package com.github.tarao

package object collection {
  type IterableLike[+A, +CC[_], +C] = scala.collection.IterableOps[A, CC, C]
  type BuildFrom[-From, -A, +C] = scala.collection.BuildFrom[From, A, C]
}
