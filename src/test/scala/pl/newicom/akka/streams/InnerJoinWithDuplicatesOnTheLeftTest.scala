package pl.newicom.akka.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import pl.newicom.akka.streams.StreamJoin.{StreamOps, nonUniqueKey, uniqueKey}

import scala.concurrent.Future

class InnerJoinWithDuplicatesOnTheLeftTest extends TestKit(ActorSystem("test")) with AsyncWordSpecLike with Matchers {
  "inner join with duplicates on the left" when {

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

      "sources intersect" should {
        "produce intersection" in {
          // given
          val left  = Source(Seq(1, 2, 3, 4, 5))
          val right = Source(Seq(4, 5, 6))

          // when/then
          innerJoin(left, right) map (_ shouldBe Seq(4, 5))
        }
      }

      "right is subset of left" should {
        "produce intersection" in {
          // given
          val left  = Source(Seq(1, 2, 3, 4, 5))
          val right = Source(Seq(2, 3, 4))

          // when/then
          innerJoin(left, right) map (_ shouldBe Seq(2, 3, 4))
        }
      }

      "left is subset of right" should {
        "produce intersection" in {
          // given
          val left  = Source(Seq(2, 3, 4))
          val right = Source(Seq(1, 2, 3, 4, 5))

          // when/then
          innerJoin(left, right) map (_ shouldBe Seq(2, 3, 4))
        }
      }
    }

    "duplicates in the left source" when {

      "sources intersect" should {
        "produce intersection preserving duplicates" in {
          // given
          val left  = Source(Seq(1, 1, 2, 2, 3, 3, 4, 4, 5, 5))
          val right = Source(Seq(4, 5, 6))

          // when/then
          innerJoin(left, right) map (_ shouldBe Seq(4, 4, 5, 5))
        }
      }

      "right is subset of left" should {
        "produce intersection preserving duplicates" in {
          // given
          val left  = Source(Seq(1, 1, 2, 2, 3, 3, 4, 4, 5, 5))
          val right = Source(Seq(2, 3, 4))

          // when/then
          innerJoin(left, right) map (_ shouldBe Seq(2, 2, 3, 3, 4, 4))
        }
      }

      "left is subset of right" should {
        "produce intersection preserving duplicates" in {
          // given
          val left  = Source(Seq(2, 2, 3, 3, 4, 4))
          val right = Source(Seq(1, 2, 3, 4, 5))

          // when/then
          innerJoin(left, right) map (_ shouldBe Seq(2, 2, 3, 3, 4, 4))
        }
      }
    }

  }

  private def innerJoin(leftSource: Source[Int, NotUsed], rightSource: Source[Int, NotUsed]): Future[Seq[Int]] =
    leftSource
      .asSorted(nonUniqueKey(_))
      .innerJoin(rightSource.asSorted(uniqueKey(_)))
      .map(_._1)
      .runWith(Sink.seq)
}
