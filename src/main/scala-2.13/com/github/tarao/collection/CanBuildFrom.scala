package com.github.tarao
package collection

class CanBuildFrom[-From, -A, +C](private val dummy: Unit) extends AnyVal
object CanBuildFrom {
  implicit def canBuildFrom[From, A, C]: CanBuildFrom[From, A, C] = new CanBuildFrom[From, A, C](())
}
