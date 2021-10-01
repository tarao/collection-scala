package com.github.tarao

import scala.language.higherKinds

package object collection {
  type IterableLike[+A, +CC[_], +C] = scala.collection.IterableLike[A, C]
  type CanBuildFrom[-From, -A, +C] = scala.collection.generic.CanBuildFrom[From, A, C]
}
