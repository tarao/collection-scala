tarao's collection utilities in Scala [![Build Status][CI-img]][CI] [![Maven Central][maven-img]][maven]
=====================================

Provide some extension methods and abstractions for collections (`Iterable`s).

Getting started
---------------

Add dependency in your `build.sbt` as the following.

```scala
    libraryDependencies ++= Seq(
      "com.github.tarao" %% "collection" % "1.0.0"
    )
```

The library is available on [Maven Central][maven].  Currently,
supported Scala versions are 2.12, 2.13, and 3.

Usage
-----

```scala
import com.github.tarao.collection.Implicits._

val s = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
s.orderBy(Seq(4, 5, 1, 3))(_._2).toVector
// val res0: Vector[(String, Int)] = Vector((d,4), (a,1), (c,3))
```

After this section, `It[A]` denotes the type of the original sequence.

- [Convenience](#convenience)
  - [`distinctyBy`](#distinctBy)
  - [`split`](#split)
- [Relation and order](#relation-and-order)
  - [`join`](#join)
  - [`leftJoin`](#leftJoin)
  - [`orderBy`](#orderBy)
  - [`totallyOrderBy`](#totallyOrderBy)
- [Refilling](#refilling)

Convenience
-----------

### `distinctBy[E](f: A => E): It[A]` <a name="distinctBy"></a>

Builds a new sequence from the original sequence without any duplications after applying
the transofrmation function `f`.

```scala
val s = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)
s.distinctBy(_ % 3)
// val res0: Seq[Int] = List(1, 2, 3)
```

You don't need this for `Seq` after Scala 2.13 since it is defined in `Seq`.  It may be
still useful when you want to apply it to an `Iterable`.

### `split[L, R](implicit eitherOf: A <:< Either[L, R]): (It[L], It[R])` <a name="split"></a>

Splits the original sequence into two sequences `It[L]` and `It[R]` if the original
sequence is of type `Either[L, R]`.

Relation and order
------------------

### `join[B](other: Iterable[B]): Inner[A, B, It]` <a name="join"></a>

Makes (inner) join of the original sequence and `other`.  This is analogous to `INNER
JOIN` in SQL.

Two functions `f` and `g` passed to `on` method as `on(f, g)` on the return value specify
association of elements in the two sequence.  If element `a` from the original sequence
and element `b` from `other` satisfy `f(a) == g(b)`, then the two elements are associated.

The associated pairs can retrieved by `result()`, `into()`, `toLazyList`, or methods of
`LazyList` (`map()` for example).  Elements with no association are simply dropped.  The
order of the resulting sequence preserves the order of the original sequence.

#### Example

```scala
import com.github.tarao.collection.Implicits._

val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
val s2 = Seq(1 -> 1, 4 -> 16, 3 -> 9, 6 -> 36)

locally {
  // .result() returns associated pairs
  val s = s1.join(s2).on(_._2, _._1).result
  // val s: Seq[((String, Int), (Int, Int))] = List(((a,1),(1,1)), ((c,3),(3,9)), ((d,4),(4,16)))
}

locally {
  // .into() transforms associated pairs
  val s = s1.join(s2).on(_._2, _._1).into { case (a, b) =>
    s"${a._1}:${b._2}"
  }
  // val s: Seq[String] = List(a:1, c:9, d:16)
}

locally {
  // A method of LazyList can be applied
  val s = s1.join(s2).on(_._2, _._1).map { case (a, b) =>
    s"${a._1}:${b._2}"
  }
  // val s: LazyList[String] = LazyList(<not computed>)
}
```

### `leftJoin[B](other: Iterable[B]): Left[A, B, It]` <a name="leftJoin"></a>

Makes left (outer) join of the original sequence and `other`.  This is analogous to `LEFT
(OUTER) JOIN` in SQL.

Two functions `f` and `g` passed to `on` method with a default value as `on(f, g)(default)`
on the return value specify association of elements in the two sequence.  If element `a`
from the original sequence and element `b` from `other` satisfy `f(a) == g(b)`, then the
two elements are associated.

If there is no `b` satisfying `f(a) == g(b)`, then the default value is associated with
`a`.  The default value can be specified in two forms:

- `on(f, g)(value: B)` : `(a, value)` are associated
- `on(f, g)(h: K => B)` : `(a, h(f(a)))` are associated

The associated pairs can retrieved by `result()`, `into()`, `toLazyList`, or methods of
`LazyList` (`map()` for example).  Elements in `other` with no association are simply
dropped.  The order of the resulting sequence preserves the order of the original
sequence.

#### Example

```scala
val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
val s2 = Seq(1 -> 1, 4 -> 16, 3 -> 9, 6 -> 36)

locally {
  // .result() returns associated pairs
  val s = s1.leftJoin(s2).on(_._2, _._1)(0 -> 0).result
  // val s: Seq[((String, Int), (Int, Int))] = List(((a,1),(1,1)), ((b,2),(0,0)), ((c,3),(3,9)), ((d,4),(4,16)), ((e,5),(0,0)))
}

locally {
  // .into() transforms associated pairs
  val s = s1.leftJoin(s2).on(_._2, _._1)(0 -> 0).into { case (a, b) =>
    s"${a._1}:${b._2}"
  }
  // val s: Seq[String] = List(a:1, b:0, c:9, d:16, e:0)
}

locally {
  // A method of LazyList can be applied
  val s = s1.leftJoin(s2).on(_._2, _._1)(0 -> 0).map { case (a, b) =>
    s"${a._1}:${b._2}"
  }
  // val s: LazyList[String] = LazyList(<not computed>)
}
```

### `orderBy[B](ordered: Seq[B])(implicit keyOf: A => B): LazyList[A]` <a name="orderBy"></a>

Reorders the original sequence according to some other `ordered` sequence using mapping
`keyOf`.  If element `a` in the original sequence has no counterpart `keyOf(a)` in
`ordered`, then it is dropped.

#### Example

```scala
val s = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
s.orderBy(Seq(4, 5, 1, 3))(_._2).toVector
// val res0: Vector[(String, Int)] = Vector((d,4), (a,1), (c,3))
```

### `totallyOrderBy[B](ordered: Seq[B])(default: B => A)(implicit keyOf: A => B): LazyList[A]` <a name="totallyOrderBy"></a>

Reorders the original sequence according to some other `ordered` sequence using mapping
`keyOf`.  If element `a` in the original sequence has no counterpart `keyOf(a)` in
`ordered`, then it is dropped.  If element `b` in `ordered` has no counterpart `a`
satisfying `keyOf(a) == b`, then `default(b)` is used for the position of `b` in
`ordered` to fill the resulting elements.

#### Example

```scala
val s = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
s.totallyOrderBy(Seq(4, 5, 1, 3))(x => "_" -> x)(_._2).toVector
// val res0: Vector[(String, Int)] = Vector((d,4), (_,5), (a,1), (c,3))
```

Refilling
---------

`Refill` provides an abstraction to retrieve elements in small pieces of chunk, with
filtering the elements, and refill up if there is insufficient amount of elements.

Basically, there are two ways of using `Refill`, which can be mixed.

1. Retrieving elements in small pieces of chunk.

   Assume that `n` is quite large and `chunkSize` is small.  The following forms execute
   the block multiple times until the total amount of elements reaches to `n`:

   ```scala
   Refill.from(start).chunked(chunkSize) { (m, cursor) =>
     /* Calling some method that returns no less than `m` elements */
   }.take(n)
   ```

   or

   ```scala
   Refill.from(start) { (m, cursor) =>
     /* Calling some method that returns no less than `m` elements */
   }.chunked(chunkSize).take(n)
   ```

   A typical usage of this is to read records from database, where you might not want to
   read the large number of records at once.

2. Refilling after filtering.

   The following form executes the block multiple times until the total amount of elements
   reaches to `n` even if `filter` drops some elements:

   ```scala
   Refill.from(start) { (m, cursor) =>
     /* Calling some method that returns no less than `m` elements */
   }.filter(/* condition */).take(n)
   ```

   A typical usage of this is to implement a list page with some hidden items that can
   navigate to the next page.  `Refill` will maintain a cursor for the navigation for you.

The latter form lacks `chunked()` call but there is some internal chunk size automatically
decided to be something larger than `n`.  Two forms are special cases of the
following form.

```scala
Refill.from(start).chunked(chunkSize) { (m, cursor) =>
  /* Calling some method that returns no less than `m` elements */
}.filter(/* condition */).take(n)
```

Moreover, there are some other filtering methods and selecting methods.  The overall general form is the following.

<pre><code>
Refill.from(<var>start</var>)<var>[</var>.chunked(<var>chunkSize</var>)<var>]</var> {
  <var>&lt;filling block&gt;</var>
}
  <var>[</var>.chunked(<var>chunkSize</var>)<var>]</var>
  <var>[</var>.<var>&lt;filtering method&gt;...]</var>
  .<var>&lt;selecting method&gt;
</code></pre>

### Filling block

```scala
{ (m, cursor) =>
  /* Calling some method that returns no less than `m` elements */
}
```

The filling block fills elements for a single chunk, taking the number of elements `m` and
`cursor` to start with in the chunk.  It may be called multiple times until the total
number of elements reaches to the size specified by the selecting method.

Note that you **MUST return no less than `m` elements** unless there is no more elements.
If you return less than `m` elements, `Refill` stops refilling and marks that it reached
to the end of the element list.

### Filtering methods

The following methods taken from `LazyList` is available.

- `distinct`
- `distinctBy`
- `drop`
- `dropRight`
- `dropWhile`
- `filter`
- `filterNot`

### Selecting methods

Selecting method `take` or `takeWithNextCursor` must be called at the end of the form.
`take(n)` just takes `n` elements and `takeWithNextCursor(n)` returns a tuple of `n`
elements and a cursor.  The cursor can be passed to next call of `Refill.from()` to
retrieve more elements.

```scala
val (elements, nextCursor) = Refill.from(start) { ... }.takeWithNextCursor(n)
val (moreElements, _) = Refill.from(nextCursor) { ... }.takeWithNextCursor(n)
```

### Cursor types

There are two types of cursor `Offset` and `Cursor`.

#### `Offset`

`Offset` provides an abstraction of offset-based refilling.

```scala
import com.github.tarao.collection.{Refill, Offset}

val offset: Int = ...
val start = Offset(offset)

val n: Int = ...
val (elements, nextOffset) =
  Refill.from(start) { (m, offset) =>
    ...
  }.takeWithNextCursor(n)
```

`Offset()` takes an integer and returns a cursor that can be passed to `Refill.from()`.
In this case, the second parameter of filling block is `offset: Int`.
`takeWithNextCursor(n)` returns `Offset(0)` if there is no more element.

If you are not using filtering methods, `nextOffset` should be `Offset(n)` since you have
already taken `n` elements.  But with filtering methods, this is not the case.  For
example, if you dropped three elements, then `nextOffset` will be `Offset(n+3)`.

#### `Cursor`

`Cursor` provides an abstraction of cursors that calculated from ingredients of elements,
i.e., it uses some factory method of type `ElementType => CursorType`.

```scala
import com.github.tarao.collection.{Refill, Cursor}

type ElementType = ...
type CursorType = ...

def factory(element: ElementType): CursorType = ...

val maybeCursor: Option[CursorType] = ...
val start = Cursor(maybeCursor)(factory)

val n: Int = ...
val (elements, nextCursor) =
  Refill.from(start) { (m, cursor) =>
    ...
  }.takeWithNextCursor(n)
```

`Cursor()` takes two arguments: the cursor value `Option[CursorType]`, where `None`
indicates the very beginning, and the factory method.  The second parameter of filling
block is `cursor: CursorType`.  `takeWithNextCursor(n)` returns `Cursor(None)(factory)` if
there is no more element.

Typically, `CursorType` is a lexicographical ordering of pairs of a timestamp and some ID [^1],
where we are thinking about some timeline.  For example, suppose we have some `Entity`
with numeric ID and a timestamp as `ElementType`.  Then we can define `factory` as
follows:

```scala
type Value = ...
case class Entity(id: Long, value: Value, createdAt: DateTime)

type ElementType = Entity
type CursorType = (DateTime, Long)
def factory(element: ElementType): CursorType = (element.createdAt, element.id)
```

The ordering (ascending or descending) depends on how you define the filling block.  Note
that `nextCursor` returned by `takeWithNextCursor(n)` and `cursor` passed to the filling
block in each chunk are calculated by one past the last element.  In other words, **the
semantics of the cursor is "inclusive"**.

[^1]: You should not use a timestamp itself as a cursor because there can be the same timestamp for different elements.

License
-------

- Copyright (C) INA Lintaro et al.
- MIT License

[CI]: https://github.com/tarao/collection-scala/actions/workflows/ci.yaml
[CI-img]: https://github.com/tarao/collection-scala/actions/workflows/ci.yaml/badge.svg
[maven]: https://search.maven.org/artifact/com.github.tarao/collection_2.13
[maven-img]: https://maven-badges.herokuapp.com/maven-central/com.github.tarao/collection_2.13/badge.svg
