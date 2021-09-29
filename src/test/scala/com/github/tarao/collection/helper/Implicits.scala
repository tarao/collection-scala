package com.github.tarao
package helper

object Implicits {
  implicit class TypeOps[A](private val exp: A) extends AnyVal {
    def isTypeOf[B](implicit ev: A =:= B): Boolean = true
    def isSubtypeOf[B](implicit ev: A <:< B): Boolean = true
  }
}
