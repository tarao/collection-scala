package com.github.tarao
package collection

// making `dummy` private causes compilation error on use site
class CanBuildFrom[-From, -A, +C](protected val dummy: Unit) extends AnyVal
object CanBuildFrom {
  implicit def canBuildFrom[From, A, C]: CanBuildFrom[From, A, C] = new CanBuildFrom[From, A, C](())
}
