package pl.newicom.akka.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import pl.newicom.akka.streams.StreamJoin.{StreamOps, nonUniqueKey, uniqueKey}

import scala.concurrent.Future

class InnerJoinWithDuplicatesOnTheRightTest extends TestKit(ActorSystem("test")) with AsyncWordSpecLike with Matchers {

  "inner join with duplicates on the right" when {

    "no duplicates" when {

      "empty sequences" should {
        "handle empty sequences" in {
          // given
          val left  = Source(Seq())
          val right = Source(Seq())

          // when/then
          innerJoin(left, right) map (_ shouldBe empty)
        }

        "handle empty left sequence" in {
          // given
          val left  = Source(Seq())
          val right = Source(Seq(1))

          // when/then
          innerJoin(left, right) map (_ shouldBe empty)
        }

        "handle empty right sequence" in {
          // given
          val left  = Source(Seq(1))
          val right = Source(Seq())

          // when/then
          innerJoin(left, right) map (_ shouldBe empty)
        }

      }

      "matching element on the right exists" when {

        "sources intersect" should {
          "merge matching elements" in {
            val left  = Source(Seq(1, 2, 3, 4, 5))
            val right = Source(Seq(2, 4, 5, 6))

            // when/then
            innerJoin(left, right) map (_ shouldBe Seq((2, 2), (4, 4), (5, 5)))
          }
        }

        "right is subset of left" should {
          "merge matching elements" in {
            val left  = Source(Seq(1, 2, 3, 4, 5, 7))
            val right = Source(Seq(2, 3, 4))

            // when/then
            innerJoin(left, right) map (_ shouldBe Seq((2, 2), (3, 3), (4, 4)))
          }
        }

        "left is subset of right" should {
          "merge matching elements" in {
            val left  = Source(Seq(2, 3, 4))
            val right = Source(Seq(1, 2, 3, 4, 5))

            // when/then
            innerJoin(left, right) map (_ shouldBe Seq((2, 2), (3, 3), (4, 4)))
          }
        }
      }

    }

    "duplicates in the right source" when {

      "matching element on the right exists" when {

        "handle equal sources with duplicates on the left" in {
          // given
          val left  = Source(Seq(1, 2, 2, 3, 4))
          val right = Source(Seq(1, 2, 3, 4))

          // when/then
          innerJoin(left, right) map (_ shouldBe Seq((1, 1), (2, 2), (3, 3), (4, 4)))
        }

        "sources intersect" should {
          "produce all from the left, only mergeable from the right, preserving duplicates" in {
            val left  = Source(Seq(1, 2, 3, 4, 5))
            val right = Source(Seq(2, 2, 4, 4, 5, 5, 6, 6))

            // when/then
            innerJoin(left, right) map (_ shouldBe Seq((2, 2), (2, 2), (4, 4), (4, 4), (5, 5), (5, 5)))
          }
        }

        "right is subset of left" should {
          "produce all from the left, only mergeable from the right, preserving duplicates" in {
            val left  = Source(Seq(1, 2, 3, 4, 5, 7))
            val right = Source(Seq(2, 2, 3, 3, 4, 4))

            // when/then
            innerJoin(left, right) map (_ shouldBe Seq((2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)))
          }
        }

        "left is subset of right" should {
          "produce all from the left, only mergeable from the right, preserving duplicates" in {
            val left  = Source(Seq(2, 3, 4))
            val right = Source(Seq(1, 1, 2, 2, 3, 3, 4, 4, 5, 5))

            // when/then
            innerJoin(left, right) map (_ shouldBe Seq((2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)))
          }
        }
      }
    }
  }

  private def innerJoin(leftSource: Source[Int, NotUsed], rightSource: Source[Int, NotUsed]): Future[Seq[(Int, Int)]] =
    leftSource
      .asSorted(uniqueKey(_))
      .innerJoin(rightSource.asSorted(nonUniqueKey(_)))
      .runWith(Sink.seq)
}
