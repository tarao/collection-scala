package com.github.tarao
package collection

import org.scalatest.{Inside, Inspectors, OptionValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec

object CursorExample {
  case class Item(name: String, timestamp: Int)
}

class CursorSpec extends AnyFunSpec
    with Matchers with OptionValues with Inside with Inspectors {
  describe("Cursor") {
    import CursorExample._

    it("should be instantiated from a cursor value and its extractor") {
      val cursor = Cursor(Some(3))((item: Item) => item.timestamp)
      cursor shouldBe a[Cursor[_, _]]
      cursor.value shouldBe Some(3)
    }

    it("should zip cursor values to a sequence") {
      val toTimestamp = (item: Item) => item.timestamp
      val cursor = Cursor(Some(3))(toTimestamp)
      cursor.factory.zip(Seq(
        Item("foo", 1),
        Item("bar", 2),
        Item("baz", 3)
      )) shouldBe Seq(
        (Item("foo", 1), Cursor(Some(1))(toTimestamp)),
        (Item("bar", 2), Cursor(Some(2))(toTimestamp)),
        (Item("baz", 3), Cursor(Some(3))(toTimestamp))
      )
    }
  }

  describe("Offset") {
    import CursorExample._

    it("should be instantiated from an offset value") {
      val offset = Offset(3)
      offset shouldBe a[Cursor[_, _]]
      offset.value shouldBe 3
    }

    it("should zip cursor values to a sequence") {
      val offset = Offset(3)
      offset.factory.zip(Seq(
        Item("foo", 1),
        Item("bar", 2),
        Item("baz", 3)
      )) shouldBe Seq(
        (Item("foo", 1), Offset(3)),
        (Item("bar", 2), Offset(4)),
        (Item("baz", 3), Offset(5))
      )
    }
  }
}
