package pl.newicom.akka.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import pl.newicom.akka.streams.StreamJoin.{StreamOps, nonUniqueKey, uniqueKey}

import scala.concurrent.Future

class LeftOuterJoinTest extends TestKit(ActorSystem("test"))
      with AsyncWordSpecLike with Matchers {

  "left-outer join" should {

    "handle empty sequences" in {
      // given
      val left  = Source(Seq())
      val right = Source(Seq())

      // when/then
      join(left, right) map (_ shouldBe empty)
    }

    "handle empty left sequence" in {
      // given
      val left  = Source(Seq())
      val right = Source(Seq(1))

      // when/then
      join(left, right) map (_ shouldBe empty)
    }

    "handle empty right sequence" in {
      // given
      val left  = Source(Seq(1))
      val right = Source(Seq())

      // when/then
      join(left, right) map (_ shouldBe Seq((1, None)))
    }

    "handle empty right sequence 2" in {
      // given
      val left  = Source(Seq(1, 2))
      val right = Source(Seq())

      // when/then
      join(left, right) map (_ shouldBe Seq((1, None), (2, None)))
    }

    "handle equal sources" in {
      // given
      val left  = Source(Seq(1, 2, 3, 4))
      val right = Source(Seq(1, 2, 3, 4))

      // when/then
      join(left, right) map (_ shouldBe Seq((1, Some(1)), (2, Some(2)), (3, Some(3)), (4, Some(4))))
    }

    "handle equal sources with duplicates on the left" in {
      // given
      val left  = Source(Seq(1, 2, 2, 3, 4))
      val right = Source(Seq(1, 2, 3, 4))

      // when/then
      join(left, right) map (_ shouldBe Seq((1, Some(1)), (2, Some(2)), (2, Some(2)), (3, Some(3)), (4, Some(4))))
    }

    "handle equal sources with duplicates on the right" in {
      // given
      val left  = Source(Seq(1, 2, 3, 4))
      val right = Source(Seq(1, 2, 2, 3, 4))

      // when/then
      join(left, right) map (_ shouldBe Seq((1, Some(1)), (2, Some(2)), (3, Some(3)), (4, Some(4))))
    }

    "emit None at the beginning" in {
      // given
      val left  = Source(Seq(1, 2, 3, 4))
      val right = Source(Seq(2, 3, 4))

      // when/then
      join(left, right) map (_ shouldBe Seq((1, None), (2, Some(2)), (3, Some(3)), (4, Some(4))))
    }

    "emit None at the end if no more elements on the right" in {
      // given
      val left  = Source(Seq(1, 2, 3, 4))
      val right = Source(Seq(1, 2, 3))

      // when/then
      join(left, right) map (_ shouldBe Seq((1, Some(1)), (2, Some(2)), (3, Some(3)), (4, None)))
    }

    "emit None at the end if no matching element on the right" in {
      // given
      val left  = Source(Seq(1, 2, 3, 4))
      val right = Source(Seq(1, 2, 3, 5))

      // when/then
      join(left, right) map (_ shouldBe Seq((1, Some(1)), (2, Some(2)), (3, Some(3)), (4, None)))
    }

    "ignore non-matching elements at the beginning of the right" in {
      // given
      val left  = Source(Seq(2, 3, 4))
      val right = Source(Seq(1, 2, 3, 4))

      // when/then
      join(left, right) map (_ shouldBe Seq((2, Some(2)), (3, Some(3)), (4, Some(4))))
    }

    "ignore non-matching elements at the end of the right" in {
      // given
      val left  = Source(Seq(1, 2, 3))
      val right = Source(Seq(1, 2, 3, 4))

      // when/then
      join(left, right) map (_ shouldBe Seq((1, Some(1)), (2, Some(2)), (3, Some(3))))
    }
  }

  private def join(leftSource: Source[Int, NotUsed], rightSource: Source[Int, NotUsed]): Future[Seq[(Int, Option[Int])]] =
    leftSource.asSorted(nonUniqueKey(_))
      .leftOuterJoin(rightSource.asSorted(uniqueKey(_)))
      .runWith(Sink.seq)

}
