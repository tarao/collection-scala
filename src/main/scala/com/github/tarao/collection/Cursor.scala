package com.github.tarao
package collection

case class Cursor[C, T](value: C, factory: Cursor.Factory[C, T])
object Cursor {
  private[collection] sealed trait Factory[C, T] {
    type Next = Cursor[C, T]
    def zip[S](s: Seq[S])(implicit upcast: S <:< T): Seq[(S, Next)]
    def beginning: C
  }
  private[collection] object Factory {
    case class Cursor[C, T](f: T => C) extends Factory[Option[C], T] {
      def zip[S](s: Seq[S])(implicit upcast: S <:< T): Seq[(S, Next)] =
        s.map(x => (x, collection.Cursor(Some(f(upcast(x))), this)))
      def beginning: Option[C] = None
    }

    case class Offset(start: Int) extends Factory[Int, Any] {
      def zip[S](s: Seq[S])(implicit upcast: S <:< Any): Seq[(S, Next)] =
        s.zipWithIndex.map { case (x, i) =>
          val next = start + i
          (x, collection.Offset(next))
        }
      def beginning: Int = 0
    }
  }

  def apply[C, T](value: Option[C])(f: T => C): Cursor[Option[C], T] =
    apply(value, Factory.Cursor(f))
}
object Offset {
  def apply(n: Int): Cursor[Int, Any] = Cursor(n, Cursor.Factory.Offset(n))
}
