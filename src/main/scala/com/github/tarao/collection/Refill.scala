package com.github.tarao
package collection

import collection.Implicits.IterableOps

/** A utility to retrieve some amount of elements with refilling to
  * reach the total amount even if some of them are dropped.
  *
  * There are mainly two forms according to usages:
  *
  *  - To retrieve the entire stream in multiple chunks; the
  *    retrieving method is called for each chunk not for the entire
  *    stream.
  *    - `Refill.from(start).chunked(chunkSize) { (limit, cursor) => /* retrieving method */ }` or
  *    - `Refill.from(start) { (limit, cursor) => /* retrieving method */ }.chunked(chunkSize)`
  *  - Just to fill the elements by calling the retrieving method many times to reach the total amount.
  *    - `Refill.from(start) { (limit, cursor) => /* retrieving method */ }`
  *
  * The only difference in the two forms is `.chunked(chunkSize)` but
  * the latter form is actually a kind of syntactic sugar of the
  * former form rather than just omitting the call of `.chunked()`.
  * It decides `chunkSize` lazily by the number provided by successive
  * call of `.take()` or `.takeWithNextCursor()`.  The former form
  * straightly uses the speicified `chunkSize`.
  *
  * In every form, it decides `chunkSize` before handling `.take(n)`
  * or `.takeWithNextCursor(n)` and first calls the retrieving method
  * with `limit` and `cursor`, which are decided by `chunkSize` and
  * `start`.  `limit` is `chunkSize + 1`, which specifies an extra +1
  * element to determine if there is more elements in the stream after
  * the chunk.  `start`, which is a [[com.github.tarao.collection.Cursor]],
  * indicates the starting position from which the elements are
  * retrieved.  `cursor` is `start.value`, which is a representation
  * of the cursor.
  *
  * After the first call of the retrieving method, it is called again
  * if the number of elements retrieved is insufficient for `n`, which
  * is the number given to `.take()` or `.takeWithNextCursor()`.  This
  * happens when:
  *
  *  - the chunk size is less than `n`, or
  *  - the number of elements became smaller than `n` because some
  *    elements are drop by `.filter()`, `.distinct()`, `.drop()`,
  *    etc.
  *
  * To each call of the retrieving method, the position of one past
  * the last element retrieved by the last call and `limit`, which is
  * the same as the last call, are given.
  *
  * Note that if your retrieving method returned a list whose size is
  * less than `limit`, then `Refill` regards it as the end of the
  * stream and will not call the retrieving method again.
  *
  * In addition to the resulting elements, `.takeWithNextCursor()`
  * returns the position of one past the last element of the entire
  * resulting elements.  If there is no more element, the position
  * will be the beginning position indicated by
  * `Cursor.factory.beginning`.
  *
  * @see [[com.github.tarao.collection.Cursor]]
  * @see [[scala.collection.immutable.LazyList]]
  */
trait Refill[A, C] {
  def mapStream[B](
    f: LazyList[(A, Cursor[C, _])] => LazyList[(B, Cursor[C, _])]
  ): Refill[B, C]

  def take(n: Refill.Limit): Seq[A]

  def takeWithNextCursor(n: Refill.Limit): (Seq[A], C)

  def collect[B](pf: PartialFunction[A, B]): Refill[B, C] =
    mapStream(_.collect { case (item, cursor) if pf.isDefinedAt(item) =>
      (pf(item), cursor)
    })

  def distinct: Refill[A, C] = distinctBy(identity)

  def distinctBy[E](f: A => E): Refill[A, C] =
    mapStream(_.distinctBy(f.compose(_._1)))

  def drop(n: Int): Refill[A, C] = mapStream(_.drop(n))

  def dropRight(n: Int): Refill[A, C] = mapStream(_.dropRight(n))

  def dropWhile(cond: A => Boolean): Refill[A, C] =
    mapStream(_.dropWhile(cond.compose(_._1)))

  def filter(cond: A => Boolean): Refill[A, C] =
    mapStream(_.filter(cond.compose(_._1)))

  def filterNot(cond: A => Boolean): Refill[A, C] =
    mapStream(_.filterNot(cond.compose(_._1)))

  def map[B, That](f: A => B): Refill[B, C] =
    mapStream(_.map { case (item, cursor) => (f(item), cursor) })
}

object Refill {
  type Limit = Int

  def from[T, U, C](initialCursor: Cursor[C, U]): From[T, U, C] =
    new From(initialCursor)

  private def refill[T, U, C](
    from: Cursor[C, U],
    chunkSize: Limit
  )(block: (Limit, C) => Seq[T])(implicit
    upcast: T <:< U
  ): Refill[T, C] = {
    var next: Option[Cursor[C, U]] = Some(from)
    val s = LazyList.continually { next.map { cursor =>
      val v = cursor.factory.zip(block(chunkSize + 1, cursor.value))
      next = v.drop(chunkSize).headOption.map(_._2)
      v.take(chunkSize)
    }.getOrElse(Seq.empty) }.takeWhile(_.nonEmpty).flatten
    new Result(s, from.factory.beginning)
  }

  private final val BufferingScaleFactor: Double = 1.5

  protected class From[T, U, C](from: Cursor[C, U]) {
    def chunked(
      chunkSize: Limit
    )(block: (Limit, C) => Seq[T])(implicit
      upcast: T <:< U
    ): Refill[T, C] = refill(from, chunkSize)(block)

    def apply(block: (Limit, C) => Seq[T])(implicit
      upcast: T <:< U
    ): Lazy[T, T, U, C] = new Lazy(from, block, (limit: Limit) => {
      (limit * BufferingScaleFactor).toInt
    })(identity)
  }

  private class Result[A, C](stream: LazyList[(A, Cursor[C, _])], beginning: C)
      extends Refill[A, C] {
    def mapStream[B](
      f: LazyList[(A, Cursor[C, _])] => LazyList[(B, Cursor[C, _])]
    ): Refill[B, C] = new Result(f(stream), beginning)

    def take(n: Refill.Limit): Seq[A] = stream.map(_._1).take(n)

    def takeWithNextCursor(n: Refill.Limit): (Seq[A], C) = {
      val s = stream.take(n + 1)
      val next = s.drop(n).headOption.map(_._2.value).getOrElse(beginning)
      (s.map(_._1).take(n), next)
    }
  }

  protected class Lazy[A, R, U, C](
    from: Cursor[C, U],
    block: (Limit, C) => Seq[A],
    reserve: Limit => Limit
  )(conv: Refill[A, C] => Refill[R, C])(implicit upcast: A <:< U)
      extends Refill[R, C] {
    def reserve(reserve: Limit => Limit): Lazy[A, R, U, C] =
      new Lazy(from, block, reserve)(conv)

    def chunked(chunkSize: Limit): Refill[R, C] =
      conv(refill(from, chunkSize)(block))

    def mapStream[B](
      f: LazyList[(R, Cursor[C, _])] => LazyList[(B, Cursor[C, _])]
    ): Refill[B, C] =
      new Lazy(from, block, reserve)(conv.andThen(_.mapStream(f)))

    private def gen(n: Limit): Refill[R, C] = chunked(reserve(n))

    def take(n: Limit): Seq[R] = gen(n).take(n)

    def takeWithNextCursor(n: Limit): (Seq[R], C) = gen(n).takeWithNextCursor(n)
  }
}
