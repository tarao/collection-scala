package com.github.tarao
package collection

import org.scalatest.{Inside, Inspectors, OptionValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec

class RefillExample {
  import RefillExample._

  private var count: Int = 0

  def called: Int = count

  def withCount[T](block: => T): T = {
    count += 1
    block
  }

  def findAll(limit: Int, offset: Int): Seq[Item] = withCount {
    dataSet.drop(offset).take(limit)
  }

  def findAllByTimestamp(limit: Int, cursor: Option[Int]): Seq[Item] =
    withCount {
      dataSet.dropWhile(x => x.timestamp > cursor.getOrElse(0)).take(limit)
    }
}
object RefillExample {
  case class Item(
    name: String,
    timestamp: Int
  )
  val toTimestamp = (item: Item) => item.timestamp

  val dataSet = Seq(
    Item("foo1", 999999),
    Item("bar1", 988888),
    Item("baz1", 977777),
    Item("qux1", 966666),
    Item("foo2", 899999),
    Item("bar2", 888888),
    Item("baz2", 877777),
    Item("qux2", 866666),
    Item("foo3", 799999),
    Item("bar3", 788888),
    Item("baz3", 777777),
    Item("qux3", 766666),
    Item("foo4", 699999),
    Item("bar4", 688888),
    Item("baz4", 677777),
    Item("qux4", 666666),
    Item("foo5", 599999),
    Item("bar5", 588888),
    Item("baz5", 577777),
    Item("qux5", 566666)
  )
}

class RefillSpec extends AnyFunSpec
    with Matchers with OptionValues with Inside with Inspectors {
  describe("RefillExample") {
    import RefillExample._

    it("should return elements by limit and offset") {
      locally {
        val example = new RefillExample
        example.findAll(3, 0) shouldBe Seq(
          Item("foo1", 999999),
          Item("bar1", 988888),
          Item("baz1", 977777)
        )
      }

      locally {
        val example = new RefillExample
        example.findAll(5, 3) shouldBe Seq(
          Item("qux1", 966666),
          Item("foo2", 899999),
          Item("bar2", 888888),
          Item("baz2", 877777),
          Item("qux2", 866666)
        )
      }

      locally {
        val example = new RefillExample
        example.findAll(5, 18) shouldBe Seq(
          Item("baz5", 577777),
          Item("qux5", 566666)
        )
      }
    }
  }

  describe("Refill (chunked) with offset") {
    describe(".take()") {
      import RefillExample._

      it("should refill elements until a limit") {
        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(7) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(3) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          )
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          )
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(6).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888)
          )
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(20).toVector shouldBe dataSet
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(25).toVector shouldBe dataSet
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(20).toVector shouldBe dataSet
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(25).toVector shouldBe dataSet
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(20).toVector shouldBe dataSet
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(25) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(20).toVector shouldBe dataSet
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(25).toVector shouldBe dataSet
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(25) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(25).toVector shouldBe dataSet
          example.called shouldBe 1
        }
      }

      it("should refill elements until a limit from an offset") {
        locally {
          val example = new RefillExample
          Refill.from(Offset(3)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(3)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(3)).chunked(7) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(3)).chunked(3) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          )
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(3)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          )
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(3)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(6).toVector shouldBe Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999)
          )
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(15).toVector shouldBe dataSet.drop(5)
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(25).toVector shouldBe dataSet.drop(5)
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(15).toVector shouldBe dataSet.drop(5)
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(25).toVector shouldBe dataSet.drop(5)
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(15) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(15).toVector shouldBe dataSet.drop(5)
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(15).toVector shouldBe dataSet.drop(5)
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(15) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(20).toVector shouldBe dataSet.drop(5)
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(20).toVector shouldBe dataSet.drop(5)
          example.called shouldBe 1
        }
      }
    }

    describe(".takeWithNextCursor()") {
      import RefillExample._

      it("should refill elements until a limit") {
        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          ), 5))
          // It needs an extra query to determine if the next element
          // exists.  This is not efficient but it is possible to avoid
          // the inefficiency by specifying a larger `chunkdSize` than
          // limit passed to `takeWithNextCursor`.  (The next test case
          // ensures this.)
          example.called shouldBe (1 + 1)
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          ), 5))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(7) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          ), 5))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(3) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          ), 5))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          ), 5))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(6)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888)
          ), 6))
          example.called shouldBe (3 + 1)
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(20)

          (s.toVector, next) shouldBe ((dataSet, 0))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(25)

          (s.toVector, next) shouldBe ((dataSet, 0))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(20)

          (s.toVector, next) shouldBe ((dataSet, 0))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(25)

          (s.toVector, next) shouldBe ((dataSet, 0))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(20)

          (s.toVector, next) shouldBe ((dataSet, 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(25) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(20)

          (s.toVector, next) shouldBe ((dataSet, 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(25)

          (s.toVector, next) shouldBe ((dataSet, 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(25) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(25)

          (s.toVector, next) shouldBe ((dataSet, 0))
          example.called shouldBe 1
        }
      }

      it("should refill elements until a limit from an offset") {
        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          ), 8))
          // It needs an extra query to determine if the next element
          // exists.  This is not efficient but it is possible to avoid
          // the inefficiency by specifying a larger `chunkdSize` than
          // limit passed to `takeWithNextCursor`.  (The next test case
          // ensures this.)
          example.called shouldBe (1 + 1)
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          ), 8))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)).chunked(7) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          ), 8))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)).chunked(3) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          ), 8))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          ), 8))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(6)

          (s.toVector, next) shouldBe ((Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999)
          ), 9))
          example.called shouldBe (3 + 1)
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(15)

          (s.toVector, next) shouldBe ((dataSet.drop(5), 0))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(25)

          (s.toVector, next) shouldBe ((dataSet.drop(5), 0))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(15)

          (s.toVector, next) shouldBe ((dataSet.drop(5), 0))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(25)

          (s.toVector, next) shouldBe ((dataSet.drop(5), 0))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(15) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(15)

          (s.toVector, next) shouldBe ((dataSet.drop(5), 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(15)

          (s.toVector, next) shouldBe ((dataSet.drop(5), 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(15) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(20)

          (s.toVector, next) shouldBe ((dataSet.drop(5), 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(20) { (limit, cursor) =>

            example.findAll(limit, cursor)
          }.takeWithNextCursor(20)

          (s.toVector, next) shouldBe ((dataSet.drop(5), 0))
          example.called shouldBe 1
        }
      }
    }

    describe(".filter().take()") {
      import RefillExample._

      it("should refill elements until a limit") {
        val dataSetBa = dataSet.filter(_.name.startsWith("ba")).toVector

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          )
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(9) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          )
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(10) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(3) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          )
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          )
          example.called shouldBe 5
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(6).toVector shouldBe Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777)
          )
          example.called shouldBe 6
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(10).toVector shouldBe dataSetBa
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(15).toVector shouldBe dataSetBa
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(10).toVector shouldBe dataSetBa
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(9).toVector shouldBe dataSetBa.dropRight(1)
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(15).toVector shouldBe dataSetBa
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(10).toVector shouldBe dataSetBa
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(15).toVector shouldBe dataSetBa
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(25) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(10).toVector shouldBe dataSetBa
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)).chunked(25) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(15).toVector shouldBe dataSetBa
          example.called shouldBe 1
        }
      }

      it("should refill elements until a limit from an offset") {
        val dataSetBa = dataSet.filter(_.name.startsWith("ba")).toVector

        locally {
          val example = new RefillExample
          Refill.from(Offset(3)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          )
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(3)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          )
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(3)).chunked(7) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          )
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(3)).chunked(3) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          )
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(3)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          )
          example.called shouldBe 6
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(3)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(6).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888),
            Item("baz4", 677777)
          )
          example.called shouldBe 6
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(8).toVector shouldBe dataSetBa.drop(2)
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(12).toVector shouldBe dataSetBa.drop(2)
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(8).toVector shouldBe dataSetBa.drop(2)
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(12).toVector shouldBe dataSetBa.drop(2)
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(8).toVector shouldBe dataSetBa.drop(2)
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(25) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(8).toVector shouldBe dataSetBa.drop(2)
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(12).toVector shouldBe dataSetBa.drop(2)
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)).chunked(25) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(12).toVector shouldBe dataSetBa.drop(2)
          example.called shouldBe 1
        }
      }
    }

    describe(".filter().takeWithNextCursor()") {
      import RefillExample._

      it("should refill elements until a limit") {
        val dataSetBa = dataSet.filter(_.name.startsWith("ba")).toVector

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          ), 10))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          ), 10))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(7) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          ), 10))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(3) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          ), 10))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          ), 10))
          example.called shouldBe 6
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(6)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777)
          ), 13))
          example.called shouldBe 7
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(10)

          (s.toVector, next) shouldBe ((dataSetBa, 0))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(15)

          (s.toVector, next) shouldBe ((dataSetBa, 0))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(10)

          (s.toVector, next) shouldBe ((dataSetBa, 0))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(8)

          (s.toVector, next) shouldBe ((dataSetBa.dropRight(2), 17))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(15)

          (s.toVector, next) shouldBe ((dataSetBa, 0))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(10)

          (s.toVector, next) shouldBe ((dataSetBa, 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(15)

          (s.toVector, next) shouldBe ((dataSetBa, 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(25) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(10)

          (s.toVector, next) shouldBe ((dataSetBa, 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)).chunked(25) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(15)

          (s.toVector, next) shouldBe ((dataSetBa, 0))
          example.called shouldBe 1
        }
      }

      it("should refill elements until a limit from an offset") {
        val dataSetBa = dataSet.filter(_.name.startsWith("ba")).toVector

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          ), 14))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          ), 14))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)).chunked(7) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          ), 14))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)).chunked(3) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          ), 14))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          ), 14))
          example.called shouldBe 6
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)).chunked(2) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(6)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888),
            Item("baz4", 677777)
          ), 17))
          example.called shouldBe 8
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(8)

          (s.toVector, next) shouldBe ((dataSetBa.drop(2), 0))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(5) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(12)

          (s.toVector, next) shouldBe ((dataSetBa.drop(2), 0))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(8)

          (s.toVector, next) shouldBe ((dataSetBa.drop(2), 0))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(6) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(12)

          (s.toVector, next) shouldBe ((dataSetBa.drop(2), 0))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(8)

          (s.toVector, next) shouldBe ((dataSetBa.drop(2), 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(25) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(8)

          (s.toVector, next) shouldBe ((dataSetBa.drop(2), 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(20) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(12)

          (s.toVector, next) shouldBe ((dataSetBa.drop(2), 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)).chunked(25) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(12)

          (s.toVector, next) shouldBe ((dataSetBa.drop(2), 0))
          example.called shouldBe 1
        }
      }
    }

    describe(".collect()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should collect matched items") {
        val s = Refill.from(Offset(0)).chunked(2) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.collect {
          case n if n % 2 == 0 => n * n
          case n if n % 3 == 0 => 2 * n
        }.take(4)

        s.toVector shouldBe Vector(16, 16, 4, 6)
      }
    }

    describe(".distinct()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should remove duplication") {
        val s = Refill.from(Offset(0)).chunked(2) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.distinct.take(4)

        s.toVector shouldBe Vector(1, 4, 2, 3)
      }
    }

    describe(".distinctBy()") {
      val testData = Seq(
        2 -> 2,
        3 -> 3,
        4 -> 2,
        5 -> 5,
        6 -> 2,
        7 -> 7,
        8 -> 2,
        9 -> 3,
        10 -> 2
      )

      it("should remove duplication") {
        val s = Refill.from(Offset(0)).chunked(2) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.distinctBy(_._2).take(4)

        // - Prefers first element
        // - Fetch needed amount when insufficient elements after
        //   deduplication
        s.toVector shouldBe Vector(
          2 -> 2,
          3 -> 3,
          5 -> 5,
          7 -> 7
        )
      }
    }

    describe(".drop()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should remove first items") {
        val s = Refill.from(Offset(0)).chunked(2) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.drop(3).take(4)

        s.toVector shouldBe Vector(4, 2, 1, 3)
      }
    }

    describe(".dropRight()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should remove last items") {
        val s = Refill.from(Offset(0)).chunked(2) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.dropRight(6).take(4)

        s.toVector shouldBe Vector(1, 4, 1)
      }
    }

    describe(".dropWhile()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should remove first items matching the condition") {
        val s = Refill.from(Offset(0)).chunked(2) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.dropWhile(x => x == 1 || x == 4).take(4)

        s.toVector shouldBe Vector(2, 1, 3, 5)
      }
    }

    describe(".filterNot()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should remove items that do not match the condition") {
        val s = Refill.from(Offset(0)).chunked(2) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.filterNot(x => x == 1 || x == 4).take(4)

        s.toVector shouldBe Vector(2, 3, 5, 6)
      }
    }

    describe(".map()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should map items") {
        val s = Refill.from(Offset(0)).chunked(2) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.map(n => n * n).take(4)

        s.toVector shouldBe Vector(1, 16, 1, 16)
      }
    }
  }

  describe("Refill (chunked) with cursors") {
    describe(".take()") {
      import RefillExample._

      it("should refill elements until a limit from a cursor") {
        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(9999999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(6) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(3) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          )
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(888888))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999),
            Item("bar3", 788888)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(889999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999),
            Item("bar3", 788888)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(888888))(toTimestamp)).chunked(6) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999),
            Item("bar3", 788888)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(888888))(toTimestamp)).chunked(3) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999),
            Item("bar3", 788888)
          )
          example.called shouldBe 2
        }
      }
    }

    describe(".takeWithNextCursor()") {
      import RefillExample._

      it("should refill elements until a limit from a cursor") {
        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.takeWithNextCursor(25)

          (s.toVector, next) shouldBe ((dataSet, None))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.takeWithNextCursor(20)

          (s.toVector, next) shouldBe ((dataSet, None))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          ), Some(888888)))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(9999999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          ), Some(888888)))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(6) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          ), Some(888888)))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(3) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          ), Some(888888)))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(888888))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999),
            Item("bar3", 788888)
          ), Some(777777)))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(889999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999),
            Item("bar3", 788888)
          ), Some(777777)))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(888888))(toTimestamp)).chunked(6) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999),
            Item("bar3", 788888)
          ), Some(777777)))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(888888))(toTimestamp)).chunked(3) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999),
            Item("bar3", 788888)
          ), Some(777777)))
          example.called shouldBe 2
        }
      }
    }

    describe("filter().take()") {
      import RefillExample._

      it("should refill elements until a limit from a cursor") {
        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          )
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(9999999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          )
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(10) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(3) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          )
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(888888))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          )
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(889999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          )
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(888888))(toTimestamp)).chunked(10) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(888888))(toTimestamp)).chunked(3) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).take(5).toVector shouldBe Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          )
          example.called shouldBe 3
        }
      }
    }

    describe("filter().takeWithNextCursor()") {
      import RefillExample._

      it("should refill elements until a limit from a cursor") {
        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("bar")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar1", 988888),
            Item("bar2", 888888),
            Item("bar3", 788888),
            Item("bar4", 688888),
            Item("bar5", 588888)
          ), None))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("bar")).takeWithNextCursor(6)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar1", 988888),
            Item("bar2", 888888),
            Item("bar3", 788888),
            Item("bar4", 688888),
            Item("bar5", 588888)
          ), None))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          ), Some(777777)))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(9999999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          ), Some(777777)))
          example.called shouldBe 3
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(6) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          ), Some(777777)))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(999999))(toTimestamp)).chunked(3) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888)
          ), Some(777777)))
          example.called shouldBe 4
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(888888))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          ), Some(677777)))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(889999))(toTimestamp)).chunked(5) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          ), Some(677777)))
          example.called shouldBe 2
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(888888))(toTimestamp)).chunked(10) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          ), Some(677777)))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(888888))(toTimestamp)).chunked(3) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name.startsWith("ba")).takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("bar3", 788888),
            Item("baz3", 777777),
            Item("bar4", 688888)
          ), Some(677777)))
          example.called shouldBe 4
        }
      }
    }

    describe(".collect()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should collect matched items") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor).chunked(2) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.collect {
          case (n, _) if n % 2 == 0 => n * n
          case (n, _) if n % 3 == 0 => 2 * n
        }.take(4)

        s.toVector shouldBe Vector(16, 16, 4, 6)
      }
    }

    describe(".distinct()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should remove duplication") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor).chunked(2) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.map(_._1).distinct.take(4)

        s.toVector shouldBe Vector(1, 4, 2, 3)
      }
    }

    describe(".distinctBy()") {
      val testData = Seq(
        2 -> 2,
        3 -> 3,
        4 -> 2,
        5 -> 5,
        6 -> 2,
        7 -> 7,
        8 -> 2,
        9 -> 3,
        10 -> 2
      ).zipWithIndex

      it("should remove duplication") {
        val cursor = Cursor(Some(0)) { (elem: ((Int, Int), Int)) => elem._2 }
        val s = Refill.from(cursor).chunked(2) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value)
        }.distinctBy(_._1._2).take(4).map(_._1)

        // - Prefers first element
        // - Fetch needed amount when insufficient elements after
        //   deduplication
        s.toVector shouldBe Vector(
          2 -> 2,
          3 -> 3,
          5 -> 5,
          7 -> 7
        )
      }
    }

    describe(".drop()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should remove first items") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor).chunked(2) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.drop(3).take(4).map(_._1)

        s.toVector shouldBe Vector(4, 2, 1, 3)
      }
    }

    describe(".dropRight()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should remove last items") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor).chunked(2) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.dropRight(6).take(4).map(_._1)

        s.toVector shouldBe Vector(1, 4, 1)
      }
    }

    describe(".dropWhile()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should remove first items matching the condition") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor).chunked(2) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.map(_._1).dropWhile(x => x == 1 || x == 4).take(4)

        s.toVector shouldBe Vector(2, 1, 3, 5)
      }
    }

    describe(".filterNot()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should remove items that do not match the condition") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor).chunked(2) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.map(_._1).filterNot(x => x == 1 || x == 4).take(4)

        s.toVector shouldBe Vector(2, 3, 5, 6)
      }
    }

    describe(".map()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should map items") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor).chunked(2) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.map { case (n, _) => n * n }.take(4)

        s.toVector shouldBe Vector(1, 16, 1, 16)
      }
    }
  }

  describe("Refill (reserved) with offset") {
    describe(".take()") {
      import RefillExample._

      it("should refill elements until a limit") {
        locally {
          val example = new RefillExample
          Refill.from(Offset(0)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(6).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(20).toVector shouldBe dataSet
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(0)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(25).toVector shouldBe dataSet
          example.called shouldBe 1
        }
      }

      it("should refill elements until a limit from an offset") {
        locally {
          val example = new RefillExample
          Refill.from(Offset(3)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(3)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(6).toVector shouldBe Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999)
          )
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(15).toVector shouldBe dataSet.drop(5)
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(25).toVector shouldBe dataSet.drop(5)
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          Refill.from(Offset(5)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.take(20).toVector shouldBe dataSet.drop(5)
          example.called shouldBe 1
        }
      }
    }

    describe(".takeWithNextCursor()") {
      import RefillExample._

      it("should refill elements until a limit") {
        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999)
          ), 5))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(6)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888)
          ), 6))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(20)

          (s.toVector, next) shouldBe ((dataSet, 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(25)

          (s.toVector, next) shouldBe ((dataSet, 0))
          example.called shouldBe 1
        }
      }

      it("should refill elements until a limit from an offset") {
        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          ), 8))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(6)

          (s.toVector, next) shouldBe ((Vector(
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999)
          ), 9))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(15)

          (s.toVector, next) shouldBe ((dataSet.drop(5), 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(25)

          (s.toVector, next) shouldBe ((dataSet.drop(5), 0))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(5)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.takeWithNextCursor(15)

          (s.toVector, next) shouldBe ((dataSet.drop(5), 0))
          example.called shouldBe 1
        }
      }
    }

    describe(".filter().take()") {
      import RefillExample._

      it("should refill elements until a limit") {
        locally {
          val example = new RefillExample
          Refill.from(Offset(0)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name != "qux1").take(5).toVector shouldBe Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("foo2", 899999),
            Item("bar2", 888888)
          )
          example.called shouldBe 1
        }
      }

      it("should refill elements until a limit from an offset") {
        locally {
          val example = new RefillExample
          Refill.from(Offset(3)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name != "qux1").take(5).toVector shouldBe Vector(
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999)
          )
          example.called shouldBe 1
        }
      }
    }

    describe(".filter().takeWithNextCursor()") {
      import RefillExample._

      it("should refill elements until a limit") {
        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(0)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name != "qux1").takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo1", 999999),
            Item("bar1", 988888),
            Item("baz1", 977777),
            Item("foo2", 899999),
            Item("bar2", 888888)
          ), 6))
          example.called shouldBe 1
        }
      }

      it("should refill elements until a limit from an offset") {
        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Offset(3)) { (limit, cursor) =>
            example.findAll(limit, cursor)
          }.filter(_.name != "qux1").takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666),
            Item("foo3", 799999)
          ), 9))
          example.called shouldBe 1
        }
      }
    }

    describe(".collect()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should collect matched items") {
        val s = Refill.from(Offset(0)) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.collect {
          case n if n % 2 == 0 => n * n
          case n if n % 3 == 0 => 2 * n
        }.take(4)

        s.toVector shouldBe Vector(16, 16, 4, 6)
      }
    }

    describe(".distinct()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should remove duplication") {
        val s = Refill.from(Offset(0)) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.distinct.take(4)

        s.toVector shouldBe Vector(1, 4, 2, 3)
      }
    }

    describe(".distinctBy()") {
      val testData = Seq(
        2 -> 2,
        3 -> 3,
        4 -> 2,
        5 -> 5,
        6 -> 2,
        7 -> 7,
        8 -> 2,
        9 -> 3,
        10 -> 2
      )

      it("should remove duplication") {
        val s = Refill.from(Offset(0)) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.distinctBy(_._2).take(4)

        // - Prefers first element
        // - Fetch needed amount when insufficient elements after
        //   deduplication
        s.toVector shouldBe Vector(
          2 -> 2,
          3 -> 3,
          5 -> 5,
          7 -> 7
        )
      }
    }

    describe(".drop()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should remove first items") {
        val s = Refill.from(Offset(0)) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.drop(3).take(4)

        s.toVector shouldBe Vector(4, 2, 1, 3)
      }
    }

    describe(".dropRight()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should remove last items") {
        val s = Refill.from(Offset(0)) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.dropRight(6).take(4)

        s.toVector shouldBe Vector(1, 4, 1)
      }
    }

    describe(".dropWhile()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should remove first items matching the condition") {
        val s = Refill.from(Offset(0)) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.dropWhile(x => x == 1 || x == 4).take(4)

        s.toVector shouldBe Vector(2, 1, 3, 5)
      }
    }

    describe(".filterNot()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should remove items that do not match the condition") {
        val s = Refill.from(Offset(0)) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.filterNot(x => x == 1 || x == 4).take(4)

        s.toVector shouldBe Vector(2, 3, 5, 6)
      }
    }

    describe(".map()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)

      it("should map items") {
        val s = Refill.from(Offset(0)) { (lim, cur) =>
          testData.drop(cur).take(lim)
        }.map(n => n * n).take(4)

        s.toVector shouldBe Vector(1, 16, 1, 16)
      }
    }
  }

  describe("Refill (reserved) with cursors") {
    describe(".take()") {
      import RefillExample._

      it("should refill elements until a limit from a cursor") {
        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(977777))(toTimestamp)) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.take(5).toVector shouldBe Vector(
            Item("baz1", 977777),
            Item("qux1", 966666),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777)
          )
          example.called shouldBe 1
        }
      }
    }

    describe(".takeWithNextCursor()") {
      import RefillExample._

      it("should refill elements until a limit from a cursor") {
        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(999999))(toTimestamp)) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.takeWithNextCursor(25)

          (s.toVector, next) shouldBe ((dataSet, None))
          example.called shouldBe 1
        }

        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(999999))(toTimestamp)) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.takeWithNextCursor(20)

          (s.toVector, next) shouldBe ((dataSet, None))
          example.called shouldBe 1
        }
      }
    }

    describe("filter().take()") {
      import RefillExample._

      it("should refill elements until a limit from a cursor") {
        locally {
          val example = new RefillExample
          Refill.from(Cursor(Some(977777))(toTimestamp)) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name != "qux1").take(5).toVector shouldBe Vector(
            Item("baz1", 977777),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          )
          example.called shouldBe 1
        }
      }
    }

    describe("filter().takeWithNextCursor()") {
      import RefillExample._

      it("should refill elements until a limit from a cursor") {
        locally {
          val example = new RefillExample
          val (s, next) = Refill.from(Cursor(Some(977777))(toTimestamp)) { (limit, cursor) =>
            example.findAllByTimestamp(limit, cursor)
          }.filter(_.name != "qux1").takeWithNextCursor(5)

          (s.toVector, next) shouldBe ((Vector(
            Item("baz1", 977777),
            Item("foo2", 899999),
            Item("bar2", 888888),
            Item("baz2", 877777),
            Item("qux2", 866666)
          ), Some(799999)))
          example.called shouldBe 1
        }
      }
    }

    describe(".collect()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should collect matched items") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.collect {
          case (n, _) if n % 2 == 0 => n * n
          case (n, _) if n % 3 == 0 => 2 * n
        }.take(4)

        s.toVector shouldBe Vector(16, 16, 4, 6)
      }
    }

    describe(".distinct()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should remove duplication") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.map(_._1).distinct.take(4)

        s.toVector shouldBe Vector(1, 4, 2, 3)
      }
    }

    describe(".distinctBy()") {
      val testData = Seq(
        2 -> 2,
        3 -> 3,
        4 -> 2,
        5 -> 5,
        6 -> 2,
        7 -> 7,
        8 -> 2,
        9 -> 3,
        10 -> 2
      ).zipWithIndex

      it("should remove duplication") {
        val cursor = Cursor(Some(0)) { (elem: ((Int, Int), Int)) => elem._2 }
        val s = Refill.from(cursor) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value)
        }.distinctBy(_._1._2).take(4).map(_._1)

        // - Prefers first element
        // - Fetch needed amount when insufficient elements after
        //   deduplication
        s.toVector shouldBe Vector(
          2 -> 2,
          3 -> 3,
          5 -> 5,
          7 -> 7
        )
      }
    }

    describe(".drop()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should remove first items") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.drop(3).take(4).map(_._1)

        s.toVector shouldBe Vector(4, 2, 1, 3)
      }
    }

    describe(".dropRight()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should remove last items") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.dropRight(6).take(4).map(_._1)

        s.toVector shouldBe Vector(1, 4, 1)
      }
    }

    describe(".dropWhile()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should remove first items matching the condition") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.map(_._1).dropWhile(x => x == 1 || x == 4).take(4)

        s.toVector shouldBe Vector(2, 1, 3, 5)
      }
    }

    describe(".filterNot()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should remove items that do not match the condition") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.map(_._1).filterNot(x => x == 1 || x == 4).take(4)

        s.toVector shouldBe Vector(2, 3, 5, 6)
      }
    }

    describe(".map()") {
      val testData = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6).zipWithIndex

      it("should map items") {
        val cursor = Cursor(Some(0)) { (elem: (Int, Int)) => elem._2 }
        val s = Refill.from(cursor) { (lim, cur) =>
          testData.dropWhile(_._2 != cur.value).take(lim)
        }.map { case (n, _) => n * n }.take(4)

        s.toVector shouldBe Vector(1, 16, 1, 16)
      }
    }
  }
}
