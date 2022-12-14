package pl.newicom.akka.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import pl.newicom.akka.streams.StreamJoin.{StreamOps, uniqueKey}

import scala.concurrent.Future

class FullJoinTest extends TestKit(ActorSystem("test")) with AsyncWordSpecLike with Matchers {

  "full join" should {
    "handle empty sequences" in {
      // given
      val left  = Source(Seq())
      val right = Source(Seq())

      // when/then
      fullJoin(left, right) map (_ shouldBe empty)
    }

    "handle empty left sequence" in {
      // given
      val left  = Source(Seq())
      val right = Source(Seq(1))

      // when/then
      fullJoin(left, right) map (_ shouldBe Seq((None, Some(1))))
    }

    "handle empty left sequence 2" in {
      // given
      val left  = Source(Seq())
      val right = Source(Seq(1, 2))

      // when/then
      fullJoin(left, right) map (_ shouldBe Seq((None, Some(1)), (None, Some(2))))
    }

    "handle empty right sequence" in {
      // given
      val left  = Source(Seq(1))
      val right = Source(Seq())

      // when/then
      fullJoin(left, right) map (_ shouldBe Seq((Some(1), None)))
    }

    "handle equal sources" in {
      // given
      val left  = Source(Seq(1, 2, 3, 4))
      val right = Source(Seq(1, 2, 3, 4))

      // when/then
      fullJoin(left, right) map (_ shouldBe Seq((Some(1), Some(1)), (Some(2), Some(2)), (Some(3), Some(3)), (Some(4), Some(4))))
    }

    "handle equal sources with duplicates on the left" in {
      // given
      val left  = Source(Seq(1, 2, 2, 3, 4))
      val right = Source(Seq(1, 2, 3, 4))

      // when/then
      fullJoin(left, right) map (_ shouldBe Seq(
        (Some(1), Some(1)),
        (Some(2), Some(2)),
        (Some(2), None),
        (Some(3), Some(3)),
        (Some(4), Some(4))
      ))
    }

    "handle equal sources with duplicates on the right" in {
      // given
      val left  = Source(Seq(1, 2, 3, 4))
      val right = Source(Seq(1, 2, 2, 3, 4))

      // when/then
      fullJoin(left, right) map (_ shouldBe Seq(
        (Some(1), Some(1)),
        (Some(2), Some(2)),
        (None, Some(2)),
        (Some(3), Some(3)),
        (Some(4), Some(4))
      ))
    }

    "emit None at the beginning" in {
      // given
      val left  = Source(Seq(1, 2, 3, 4))
      val right = Source(Seq(2, 3, 4))

      // when/then
      fullJoin(left, right) map (_ shouldBe Seq((Some(1), None), (Some(2), Some(2)), (Some(3), Some(3)), (Some(4), Some(4))))
    }

    "emit None at the end if no more elements on the right" in {
      // given
      val left  = Source(Seq(1, 2, 3, 4))
      val right = Source(Seq(1, 2, 3))

      // when/then
      fullJoin(left, right) map (_ shouldBe Seq((Some(1), Some(1)), (Some(2), Some(2)), (Some(3), Some(3)), (Some(4), None)))
    }

    "work 1" in {
      // given
      val left  = Source(Seq(1, 2, 4, 6))
      val right = Source(Seq(1, 3, 5, 7))

      // when/then
      fullJoin(left, right) map (_ shouldBe Seq(
        (Some(1), Some(1)),
        (Some(2), None),
        (None, Some(3)),
        (Some(4), None),
        (None, Some(5)),
        (Some(6), None),
        (None, Some(7))
      ))
    }

    "work 2" in {
      // given
      val left  = Source(Seq(2, 3, 4))
      val right = Source(Seq(1, 2, 3, 4))

      // when/then
      fullJoin(left, right) map (_ shouldBe Seq((None, Some(1)), (Some(2), Some(2)), (Some(3), Some(3)), (Some(4), Some(4))))
    }

    "work 3" in {
      // given
      val left  = Source(Seq(1, 2, 4, 8))
      val right = Source(Seq(1, 3, 5, 7))

      // when/then
      fullJoin(left, right) map (_ shouldBe Seq(
        (Some(1), Some(1)),
        (Some(2), None),
        (None, Some(3)),
        (Some(4), None),
        (None, Some(5)),
        (None, Some(7)),
        (Some(8), None)
      ))
    }

    "emit None at the end if no matching element on the right" in {
      // given
      val left  = Source(Seq(1, 2, 3, 4))
      val right = Source(Seq(1, 2, 3, 5, 6, 7))

      // when/then
      fullJoin(left, right) map (_ shouldBe Seq(
        (Some(1), Some(1)),
        (Some(2), Some(2)),
        (Some(3), Some(3)),
        (Some(4), None),
        (None, Some(5)),
        (None, Some(6)),
        (None, Some(7))
      ))
    }

  }

  private def fullJoin(
    leftSource: Source[Int, NotUsed],
    rightSource: Source[Int, NotUsed]
  ): Future[Seq[(Option[Int], Option[Int])]] =
    leftSource
      .asSorted(uniqueKey(_))
      .fullJoin(rightSource.asSorted(uniqueKey(_)))
      .runWith(Sink.seq)

}
