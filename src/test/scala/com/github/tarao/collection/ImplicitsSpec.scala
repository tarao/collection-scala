package com.github.tarao
package collection

import helper.Implicits.TypeOps
import org.scalatest.{Inside, Inspectors, OptionValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec

class ImplicitsSpec extends AnyFunSpec
    with Matchers with OptionValues with Inside with Inspectors {
  describe("Implicits") {
    describe("IterableOps") {
      import Implicits.IterableOps

      describe(".distinctBy") {
        it("should uniquify elements") {
          val s = Seq(1, 2, 2, 1, 2, 2, 2)
          s.distinctBy(identity) shouldBe Seq(1, 2)
        }

        it("should uniquify elements with respect to the specified function") {
          val s = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)
          s.distinctBy(_ % 3) shouldBe Seq(1, 2, 3)
        }

        it("should preserve the collection type") {
          locally {
            val s = Seq(1, 4, 1, 4, 2, 1, 3, 5, 6)
            val r = s.distinctBy(_ % 3)
            r.isTypeOf[Seq[Int]]
            r shouldBe Seq(1, 2, 3)
          }

          locally {
            val s = List(1, 4, 1, 4, 2, 1, 3, 5, 6)
            val r = s.distinctBy(_ % 3)
            r.isTypeOf[List[Int]]
            r shouldBe List(1, 2, 3)
          }

          locally {
            val s = Vector(1, 4, 1, 4, 2, 1, 3, 5, 6)
            val r = s.distinctBy(_ % 3)
            r.isTypeOf[Vector[Int]]
            r shouldBe Vector(1, 2, 3)
          }
        }
      }

      describe(".orderBy") {
        it("should reorder by another sequence") {
          val s = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
          s.orderBy(Seq(2, 4, 1, 3))(_._2) shouldBe
            Seq("b" -> 2, "d" -> 4, "a" -> 1, "c" -> 3)
        }

        it("should skip missing values") {
          locally {
            val s = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
            s.orderBy(Seq(4, 1, 3))(_._2) shouldBe
              Seq("d" -> 4, "a" -> 1, "c" -> 3)
          }

          locally {
            val s = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
            s.orderBy(Seq(4, 5, 1, 3))(_._2) shouldBe
              Seq("d" -> 4, "a" -> 1, "c" -> 3)
          }
        }
      }

      describe(".totallyOrderBy") {
        it("should reorder by another sequence") {
          val s = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
          s.totallyOrderBy(Seq(2, 4, 1, 3))(_ => "_" -> 0)(_._2) shouldBe
            Seq("b" -> 2, "d" -> 4, "a" -> 1, "c" -> 3)
        }

        it("should fill missing values with default values") {
          locally {
            val s = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
            s.totallyOrderBy(Seq(4, 1, 3))(_ => "_" -> 0)(_._2) shouldBe
              Seq("d" -> 4, "a" -> 1, "c" -> 3)
          }

          locally {
            val s = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
            s.totallyOrderBy(Seq(4, 5, 1, 3))(x => "_" -> x)(_._2) shouldBe
              Seq("d" -> 4, "_" -> 5, "a" -> 1, "c" -> 3)
          }
        }
      }

      describe(".join().on()") {
        describe(".into()") {
          it("should join two lists by some key") {
            val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
            val s2 = Seq(2 -> 4, 4 -> 16, 1 -> 1, 3 -> 9)

            val s3 = s1.join(s2).on(_._2, _._1).into { case (a, b) =>
              a._1 -> b._2
            }
            s3.isTypeOf[Seq[(String, Int)]]
            s3 shouldBe a[Seq[_]]
            s3 shouldBe Seq("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16)
          }

          it("should skip missing values") {
            val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
            val s2 = Seq(1 -> 1, 4 -> 16, 3 -> 9, 6 -> 36)

            val s3 = s1.join(s2).on(_._2, _._1).into { case (a, b) =>
              a._1 -> b._2
            }
            s3.isTypeOf[Seq[(String, Int)]]
            s3 shouldBe a[Seq[_]]
            s3 shouldBe Seq("a" -> 1, "c" -> 9, "d" -> 16)
          }

          it("should preserve the collection type") {
            locally {
              val s1 = List("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
              val s2 = Seq(2 -> 4, 4 -> 16, 1 -> 1, 3 -> 9)

              val s3 = s1.join(s2).on(_._2, _._1).into { case (a, b) =>
                a._1 -> b._2
              }
              s3.isTypeOf[List[(String, Int)]]
              s3 shouldBe a[List[_]]
              s3 shouldBe List("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16)
            }

            locally {
              val s1 = Vector("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
              val s2 = Seq(2 -> 4, 4 -> 16, 1 -> 1, 3 -> 9)

              val s3 = s1.join(s2).on(_._2, _._1).into { case (a, b) =>
                a._1 -> b._2
              }
              s3.isTypeOf[Vector[(String, Int)]]
              s3 shouldBe a[Vector[_]]
              s3 shouldBe Vector("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16)
            }

            locally {
              val s1 = Set("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
              val s2 = Seq(2 -> 4, 4 -> 16, 1 -> 1, 3 -> 9)

              val s3 = s1.join(s2).on(_._2, _._1).into { case (a, b) =>
                a._1 -> b._2
              }
              s3.isTypeOf[Set[(String, Int)]]
              s3 shouldBe a[Set[_]]
              s3 shouldBe Set("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16)
            }

            locally {
              val s1: Iterable[(String, Int)] =
                Vector("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4).toSeq
              val s2 = Seq(2 -> 4, 4 -> 16, 1 -> 1, 3 -> 9)

              val s3 = s1.join(s2).on(_._2, _._1).into { case (a, b) =>
                a._1 -> b._2
              }
              s3.isTypeOf[Iterable[(String, Int)]]
              s3 shouldBe Seq("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16)
            }
          }
        }

        describe("LazyList methods") {
          it("should join two lists by some key") {
            val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
            val s2 = Seq(2 -> 4, 4 -> 16, 1 -> 1, 3 -> 9)

            val s3 = s1.join(s2).on(_._2, _._1).map { case (a, b) =>
              (a._1, b._2)
            }
            s3.isTypeOf[LazyList[(String, Int)]]
            s3 shouldBe Seq("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16)
          }

          it("should skip missing values") {
            val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
            val s2 = Seq(1 -> 1, 4 -> 16, 3 -> 9, 6 -> 36)

            val s3 = s1.join(s2).on(_._2, _._1).map { case (a, b) =>
              (a._1, b._2)
            }
            s3.isTypeOf[LazyList[(String, Int)]]
            s3 shouldBe Seq("a" -> 1, "c" -> 9, "d" -> 16)
          }

          it("should reduce redundant loops") {
            val called = scala.collection.mutable.ArrayBuffer[Any]()

            val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
            val s2 = Seq(1 -> 1, 4 -> 16, 3 -> 9, 6 -> 36)

            val s3 = s1.join(s2).on({ x =>
              called += x._1
              x._2
            }, _._1).map { case (a, b) =>
              called += a._1
              (a._1, b._2)
            }
            s3.isTypeOf[LazyList[(String, Int)]]
            s3 shouldBe Seq("a" -> 1, "c" -> 9, "d" -> 16)
            called shouldBe Seq("a", "a", "b", "c", "c", "d", "d", "e")
          }
        }
      }

      describe(".leftJoin().on()") {
        describe(".into()") {
          it("should join two lists by some key") {
            val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
            val s2 = Seq(2 -> 4, 4 -> 16, 1 -> 1, 3 -> 9)

            val s3 = s1.leftJoin(s2).on(_._2, _._1)(0 -> 0).into { case (a, b) =>
              a._1 -> b._2
            }
            s3.isTypeOf[Seq[(String, Int)]]
            s3 shouldBe a[Seq[_]]
            s3 shouldBe Seq("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16)
          }

          it("should fill missing values with a default value") {
            val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
            val s2 = Seq(1 -> 1, 4 -> 16, 3 -> 9, 6 -> 36)

            val s3 = s1.leftJoin(s2).on(_._2, _._1)(-1 -> 0).into { case (a, b) =>
              a._1 -> b._2
            }
            s3.isTypeOf[Seq[(String, Int)]]
            s3 shouldBe a[Seq[_]]
            s3 shouldBe Seq("a" -> 1, "b" -> 0, "c" -> 9, "d" -> 16, "e" -> 0)
          }

          it("should fill missing values with default values") {
            val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
            val s2 = Seq(1 -> 1, 4 -> 16, 3 -> 9, 6 -> 36)

            val s3 = s1.leftJoin(s2).on(_._2, _._1)(x => x -> x * 2).into { case (a, b) =>
              a._1 -> b._2
            }
            s3.isTypeOf[Seq[(String, Int)]]
            s3 shouldBe a[Seq[_]]
            s3 shouldBe Seq("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16, "e" -> 10)
          }

          it("should preserve the collection type") {
            locally {
              val s1 = List("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
              val s2 = Seq(2 -> 4, 4 -> 16, 1 -> 1, 3 -> 9)

              val s3 = s1.leftJoin(s2).on(_._2, _._1)(0 -> 0).into { case (a, b) =>
                a._1 -> b._2
              }
              s3.isTypeOf[List[(String, Int)]]
              s3 shouldBe a[List[_]]
              s3 shouldBe List("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16)
            }

            locally {
              val s1 = Vector("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
              val s2 = Seq(2 -> 4, 4 -> 16, 1 -> 1, 3 -> 9)

              val s3 = s1.leftJoin(s2).on(_._2, _._1)(0 -> 0).into { case (a, b) =>
                a._1 -> b._2
              }
              s3.isTypeOf[Vector[(String, Int)]]
              s3 shouldBe a[Vector[_]]
              s3 shouldBe Vector("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16)
            }

            locally {
              val s1 = Set("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
              val s2 = Seq(2 -> 4, 4 -> 16, 1 -> 1, 3 -> 9)

              val s3 = s1.join(s2).on(_._2, _._1).into { case (a, b) =>
                a._1 -> b._2
              }
              s3.isTypeOf[Set[(String, Int)]]
              s3 shouldBe a[Set[_]]
              s3 shouldBe Set("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16)
            }

            locally {
              val s1: Iterable[(String, Int)] =
                Vector("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4).toSeq
              val s2 = Seq(2 -> 4, 4 -> 16, 1 -> 1, 3 -> 9)

              val s3 = s1.join(s2).on(_._2, _._1).into { case (a, b) =>
                a._1 -> b._2
              }
              s3.isTypeOf[Iterable[(String, Int)]]
              s3 shouldBe Seq("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16)
            }
          }
        }

        describe("LazyList methods") {
          it("should join two lists by some key") {
            val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
            val s2 = Seq(2 -> 4, 4 -> 16, 1 -> 1, 3 -> 9)

            val s3 = s1.leftJoin(s2).on(_._2, _._1)(0 -> 0).map { case (a, b) =>
              (a._1, b._2)
            }
            s3.isTypeOf[LazyList[(String, Int)]]
            s3 shouldBe Seq("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16)
          }

          it("should fill missing values with a default value") {
            val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
            val s2 = Seq(1 -> 1, 4 -> 16, 3 -> 9, 6 -> 36)

            val s3 = s1.leftJoin(s2).on(_._2, _._1)(-1 -> 0).map { case (a, b) =>
              (a._1, b._2)
            }
            s3.isTypeOf[LazyList[(String, Int)]]
            s3 shouldBe Seq("a" -> 1, "b" -> 0, "c" -> 9, "d" -> 16, "e" -> 0)
          }

          it("should fill missing values with default values") {
            val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
            val s2 = Seq(1 -> 1, 4 -> 16, 3 -> 9, 6 -> 36)

            val s3 = s1.leftJoin(s2).on(_._2, _._1)(x => x -> x * 2).map { case (a, b) =>
              (a._1, b._2)
            }
            s3.isTypeOf[LazyList[(String, Int)]]
            s3 shouldBe Seq("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16, "e" -> 10)
          }

          it("should reduce redundant loops") {
            val called = scala.collection.mutable.ArrayBuffer[Any]()

            val s1 = Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
            val s2 = Seq(1 -> 1, 4 -> 16, 3 -> 9, 6 -> 36)

            val s3 = s1.leftJoin(s2).on({ x =>
              called += x._1
              x._2
            }, _._1)(x => x -> x * 2).map { case (a, b) =>
              called += a._1
              (a._1, b._2)
            }
            s3.isTypeOf[LazyList[(String, Int)]]
            s3 shouldBe Seq("a" -> 1, "b" -> 4, "c" -> 9, "d" -> 16, "e" -> 10)
            called shouldBe Seq("a", "a", "b", "b", "c", "c", "d", "d", "e", "e")
          }
        }
      }

      describe(".split()") {
        describe("for List[]") {
          it("should split a list of Either[]") {
            val list: List[Either[Int, String]] = List(
              Left(1),
              Right("foo"),
              Right("bar"),
              Left(2),
              Right("baz"),
              Right("qux"),
              Left(3)
            )

            val (left, right) = list.split
            left shouldBe a[List[_]]
            right shouldBe a[List[_]]
            left shouldBe List(1, 2, 3)
            right shouldBe List("foo", "bar", "baz", "qux")
          }
        }

        describe("for Seq[]") {
          it("should split a list of Either[]") {
            val seq: Seq[Either[Int, String]] = Seq(
              Left(1),
              Right("foo"),
              Right("bar"),
              Left(2),
              Right("baz"),
              Right("qux"),
              Left(3)
            )

            val (left, right) = seq.split
            left shouldBe a[Seq[_]]
            right shouldBe a[Seq[_]]
            left shouldBe Seq(1, 2, 3)
            right shouldBe Seq("foo", "bar", "baz", "qux")
          }
        }

        describe("for Vector[]") {
          it("should split a list of Either[]") {
            val vector: Vector[Either[Int, String]] = Vector(
              Left(1),
              Right("foo"),
              Right("bar"),
              Left(2),
              Right("baz"),
              Right("qux"),
              Left(3)
            )

            val (left, right) = vector.split
            left shouldBe a[Vector[_]]
            right shouldBe a[Vector[_]]
            left shouldBe Vector(1, 2, 3)
            right shouldBe Vector("foo", "bar", "baz", "qux")
          }
        }

        describe("for LazyList[]") {
          it("should split a list of Either[]") {
            val stream: LazyList[Either[Int, String]] = LazyList(
              Left(1),
              Right("foo"),
              Right("bar"),
              Left(2),
              Right("baz"),
              Right("qux"),
              Left(3)
            )

            val (left, right) = stream.split
            left shouldBe a[LazyList[_]]
            right shouldBe a[LazyList[_]]
            left shouldBe LazyList(1, 2, 3)
            right shouldBe LazyList("foo", "bar", "baz", "qux")
          }
        }

        describe("for Set[]") {
          it("should split a list of Either[]") {
            val set: Set[Either[Int, String]] = Set(
              Left(1),
              Right("foo"),
              Right("bar"),
              Left(2),
              Right("baz"),
              Right("qux"),
              Left(3)
            )

            val (left, right) = set.split
            left shouldBe a[Set[_]]
            right shouldBe a[Set[_]]
            left shouldBe Set(1, 2, 3)
            right shouldBe Set("foo", "bar", "baz", "qux")
          }
        }
      }
    }
  }
}
