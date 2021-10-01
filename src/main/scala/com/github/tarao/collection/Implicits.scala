package com.github.tarao
package collection

import collection.relation.Join
import scala.language.higherKinds

object Implicits {
  import scala.language.implicitConversions

  implicit def IterableOps[ItLike, A, It[X] <: IterableLike[X, It, It[X]]](
    it: ItLike
  )(implicit toIt: ItLike => It[A]): IterableOps[A, It] =
    new IterableOps(toIt(it))

  // This cannot be an implicit class since we need an implicit
  // paramter `ItLike => It[A]` and the class cannot be `AnyVal` with
  // the parameter.
  protected class IterableOps[A, It[X] <: IterableLike[X, It, It[X]]](
    private val it: It[A]
  ) extends AnyVal {
    /** Builds a new sequence from this sequence without any duplications
      * of mapped elements.
      *
      * @param  f  a mapping whose codomain is used to determine duplication.
      * @return    a new sequence contains the first occurrence of every element of this sequence.
      *
      * @example {{{
      *   val s = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)
      *   s.distinctBy(_ % 3)
      *   // res0: Seq[Int] = List(1, 2, 3)
      * }}}
      */
    def distinctBy[E](f: A => E): It[A] = {
      val seen = scala.collection.mutable.Set[E]()
      it.filter(elem => seen.add(f(elem)))
    }

    /** Reorder this sequence according to another sequence and a mapping
      * of element types.
      *
      *   The position of an element in this sequence is decided by
      * the position of an element in the specified sequence which is
      * mapped from the element in this sequence by the implicitly
      * specified mapping.  If an element is mapped to no element in
      * the specified sequence, then the element is eliminated from
      * the result.  If there is an element in the specified sequence
      * which has no counterpart in this sequence, then the position
      * of the element is simply skipped.
      *
      * @param ordered  a sequence which specifies the order.
      * @param keyOf    an implicit mapping from an element in this
      *                 sequence to an element in `ordered`.
      * @return         a stream whose elements are from this sequence
      *                 and ordered by `ordered`.
      * @see [[IterableOps#totallyOrderBy]]
      *
      * @example {{{
      *   val s = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
      *   s.orderBy(Seq(4, 5, 1, 3))(_._2).toVector
      *   // res0: Vector[(String, Int)] = Vector((d,4), (a,1), (c,3))
      * }}}
      */
    def orderBy[B](ordered: Seq[B])(implicit keyOf: A => B): LazyList[A] =
      ordered.join(it.toIterable).on(identity(_), keyOf).map(_._2)

    /** Reorder this sequence according to another sequence and a mapping
      * of element types.
      *
      * The position of an element in this sequence is decided by the
      * position of an element in the specified sequence which is
      * mapped from the element in this sequence by the implicitly
      * specified mapping.  If an element is mapped to no element in
      * the specified sequence, then the element is eliminated from
      * the result.  If there is an element in the specified sequence
      * which has no counterpart in this sequence, then the position
      * of the element is filled by a default value.
      *
      * @param ordered  a sequence which specifies the order.
      * @param default  a function to provide a default value.
      *                 The parameter of the function is an element in
      *                 `ordered` which has no counterpart in this
      *                 sequence.
      * @param keyOf    an implicit mapping from an element in this
      *                 sequence to an element in `ordered`.
      * @return         a stream whose elements are from this sequence
      *                 and ordered by `ordered`.
      * @see [[IterableOps#orderBy]]
      *
      * @example {{{
      *   val s = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
      *   s.totallyOrderBy(Seq(4, 5, 1, 3))(x => "_" -> x)(_._2).toVector
      *   // res0: Vector[(String, Int)] = Vector((d,4), (_,5), (a,1), (c,3))
      * }}}
      */
    def totallyOrderBy[B](ordered: Seq[B])(default: B => A)(implicit keyOf: A => B): LazyList[A] =
      ordered.leftJoin(it.toIterable).on(identity, keyOf)(default).map(_._2)

    /** Make (inner) join of this sequence and another sequence.
      *
      * Two functions passed to `on()` method on the return value
      * specifies association of elements in two sequences.  Two
      * elements are associated if the result of the functions are the
      * same.  Elements are skipped if there is no association for them.
      *
      * The return value of `on()` is an intermediate value to
      * describe the join.  You have to call `result()`, `into()` or
      * some stream method to get the result.
      *
      * The result sequence preserves the order of this sequence.
      *
      * @param  other  a sequence to join with this sequence.
      * @return        an object to call `on()` method.
      *
      * @example {{{
      *   val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
      *   val s2 = Seq(1 -> 1, 4 -> 16, 3 -> 9, 6 -> 36)
      *
      *   locally {
      *     // Get a result as a sequence of tuples:
      *     val s = s1.join(s2).on(_._2, _._1).result
      *     // s: Seq[((String, Int), (Int, Int))] = List(((a,1),(1,1)), ((c,3),(3,9)), ((d,4),(4,16)))
      *   }
      *
      *   locally {
      *     // Get a result as a sequence of what you want:
      *     val s = s1.join(s2).on(_._2, _._1).into { case (a, b) =>
      *       s"\${a._1}:\${b._2}"
      *     }
      *     // s: Seq[String] = List(a:1, c:9, d:16)
      *   }
      *
      *   locally {
      *     // You can call any `LazyList` method on the result:
      *     val s = s1.join(s2).on(_._2, _._1).map { case (a, b) =>
      *       s"\${a._1}:\${b._2}"
      *     }
      *     // s: scala.collection.immutable.LazyList[String] = LazyList(a:1, ?)
      *   }
      * }}}
      */
    def join[B](other: Iterable[B]): Join.Inner[A, B, It] =
      new Join.Inner(it, other)

    /** Make left (outer) join of this sequence and another sequence.
      *
      * Two functions passed to `on()` method on the return value
      * specifies association of elements in two sequences.  Two
      * elements are associated if the result of the functions are the
      * same.  If there is no association in the specified sequence, a
      * default value provided by the argument of `on()` is used.
      *
      * The return value of `on()` is an intermediate value to
      * describe the join.  You have to call `result()`, `into()` or
      * some stream method to get the result.
      *
      * The result sequence preserves the order of this sequence.
      *
      * @param  other  a sequence to join with this sequence.
      * @return        an object to call `on()` method.
      *
      * @example {{{
      *   val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
      *   val s2 = Seq(1 -> 1, 4 -> 16, 3 -> 9, 6 -> 36)
      *
      *   locally {
      *     // Get a result as a sequence of tuples:
      *     val s = s1.leftJoin(s2).on(_._2, _._1)(0 -> 0).result
      *     // s: Seq[((String, Int), (Int, Int))] = List(((a,1),(1,1)), ((b,2),(0,0)), ((c,3),(3,9)), ((d,4),(4,16)), ((e,5),(0,0)))
      *   }
      *
      *   locally {
      *     // Get a result as a sequence of what you want:
      *     val s = s1.leftJoin(s2).on(_._2, _._1)(0 -> 0).into { case (a, b) =>
      *       s"\${a._1}:\${b._2}"
      *     }
      *     // s: Seq[String] = List(a:1, b:0, c:9, d:16, e:0)
      *   }
      *
      *   locally {
      *     // You can call any `LazyList` method on the result:
      *     val s = s1.leftJoin(s2).on(_._2, _._1)(0 -> 0).map { case (a, b) =>
      *       s"\${a._1}:\${b._2}"
      *     }
      *     // s: scala.collection.immutable.LazyList[String] = LazyList(a:1, ?)
      *   }
      * }}}
      */
    def leftJoin[B](other: Iterable[B]): Join.Left[A, B, It] =
      new Join.Left(it, other)

    /** Split list of disjunctions into a pair of left list and right list.
      *
      * @return a pair of left list and right list.
      */
    def split[L, R, C](implicit
      eitherOf: A <:< Either[L, R],
      bfl: BuildFrom[It[A], L, It[L]],
      bfr: BuildFrom[It[A], R, It[R]],
    ): (It[L], It[R]) = {
      val ls = bfl.newBuilder(it)
      val rs = bfr.newBuilder(it)
      it.foreach { x => eitherOf(x) match {
        case Left(l)  => ls += l
        case Right(r) => rs += r
      } }
      (ls.result(), rs.result())
    }
  }
}
