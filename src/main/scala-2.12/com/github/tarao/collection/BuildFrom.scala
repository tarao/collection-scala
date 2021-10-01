package com.github.tarao
package collection

import scala.collection.mutable.Builder

trait BuildFrom[-From, -A, +C] extends Any {
  def newBuilder(from: From): Builder[A, C]
}
object BuildFrom {
  implicit def apply[From, A, C](implicit
    cbf: scala.collection.generic.CanBuildFrom[From, A, C]
  ): BuildFrom[From, A, C] = new BuildFrom[From, A, C] {
    def newBuilder(from: From): Builder[A, C] = cbf(from)
  }
}
