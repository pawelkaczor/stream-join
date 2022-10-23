package pl.newicom.akka.streams

import akka.NotUsed
import akka.stream.scaladsl.{GraphDSL, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, StageLogging}
import akka.stream.{Attributes, FanInShape2, SourceShape}
import StreamJoin._

import scala.annotation.unused
import scala.math.Ordered.orderingToOrdered

class StreamJoin[O: Ordering, LKT <: KeyType, RKT <: KeyType, L, R, LR](
    zipper: PartialFunction[(Option[L], Option[R]), LR],
    join: Join[O, LKT, RKT, L, R]
) extends GraphStage[FanInShape2[L, R, LR]] {
  override val shape: FanInShape2[L, R, LR] =
    new FanInShape2("StreamJoin")

  private val left               = shape.in0
  private val right              = shape.in1
  private val out                = shape.out
  private val joinType: JoinType = join.joinType

  private val comparator: (L, R) => Int = {
    (l, r) =>
      join.leftKey(l).asInstanceOf[JoinKey[O, KeyType]] compareTo join.rightKey(r).asInstanceOf[JoinKey[O, KeyType]]
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic with StageLogging =
    new GraphStageLogic(shape) with StageLogging {
      setHandler(left, ignoreTerminateInput)
      setHandler(right, ignoreTerminateInput)
      setHandler(out, eagerTerminateOutput)

      var leftValue: L  = _
      var rightValue: R = _

      def dispatch(): Unit = {
        val c = comparator.apply(leftValue, rightValue)
        if (c < 0) {
          if (joinType == Full || joinType == LeftOuter) {
            tryEmit(Some(leftValue), None)
          } else {
            readL()
          }
        } else if (c == 0) {
          tryEmit(Some(leftValue), Some(rightValue))
        } else { // c > 0
          if (joinType == Full) {
            tryEmit(None, Some(rightValue))
          } else {
            readR()
          }
        }
      }

      private def tryEmit(l: Option[L], r: Option[R], andThen: => Unit = readNext()): Unit =
        if (zipper.isDefinedAt((l, r))) {
          emit(out, zipper((l, r)), () => andThen)
        } else {
          andThen
        }

      private def readNext(): Unit =
        if (joinType == Full) {
          val c = comparator.apply(leftValue, rightValue)
          if (c < 0) {
            readL()
          } else if (c == 0) {
            readL { _ =>
              readR(_ => dispatch())
            }
          } else {
            readR()
          }
        } else {
          (join.leftKeyType(leftValue), join.rightKeyType(rightValue)) match {
            case (UniqueKey, UniqueKey)    => readL()
            case (UniqueKey, NonUniqueKey) => readR()
            case (NonUniqueKey, UniqueKey) => readL()
            case _                         => readL(_ => readR())
          }
        }

      def dispatchR(@unused v: R): Unit = {
        dispatch()
      }

      def dispatchL(@unused v: L): Unit = {
        dispatch()
      }

      def readR(andThen: R => Unit = dispatchR): Unit = read(right)(
        r => {
          rightValue = r
          andThen(r)
        },
        onClose = () =>
          joinType match {
            case Inner =>
              completeStage()
            case _ =>
              if (comparator(leftValue, rightValue) <= 0) {
                readL()
              } else {
                tryEmit(Some(leftValue), None, readL())
              }
          }
      )

      def readL(andThen: L => Unit = dispatchL): Unit = read(left)(
        l => {
          leftValue = l
          andThen(l)
        },
        onClose = () =>
          if (joinType == Full && comparator.apply(leftValue, rightValue) < 0) {
            tryEmit(None, Some(rightValue), read(right)(r => { rightValue = r; readL(andThen) }, () => completeStage()))
          } else {
            completeStage()
          }
      )

      private def start(): Unit = read(left)(
        andThen = lv => {
          leftValue = lv
          read(right)(
            andThen = { rv =>
              rightValue = rv
              dispatchR(rv)
            },
            onClose = () =>
              tryEmit(Some(leftValue), None, start())
          )
        },
        onClose = () => {
          if (joinType == Full) {
            read(right)(
              andThen = { rv =>
                rightValue = rv
                tryEmit(None, Some(rv), start())
              },
              onClose = () => completeStage()
            )
          } else {
            completeStage()
          }
        }
      )

      override def preStart(): Unit = {
        // all fan-in stages need to eagerly pull all inputs to get cycles started
        pull(right)
        start()
      }

    }
}

object StreamJoin {
  implicit class StreamOps[E](source: Source[E, NotUsed]) {
    def asSorted[O: Ordering, KT <: KeyType](key: E => JoinKey[O, KT]): SortedSource[O, KT, E] =
      SortedSource(source, key)
  }

  implicit class SortedStreamOps[O: Ordering, LKT <: KeyType, L](leftSource: SortedSource[O, LKT, L]) {
    def innerJoin[R, RKT <: KeyType](rightSource: SortedSource[O, RKT, R]): Source[(L, R), NotUsed] =
      StreamJoin[O, LKT, RKT, L, R, (L, R)](
        leftSource.source,
        rightSource.source,
        Join(leftSource.key, rightSource.key, Inner),
        zipper = {
          case (Some(l), Some(r)) => (l, r)
        }
      )

    def leftOuterJoin[R, RKT <: KeyType](rightSource: SortedSource[O, RKT, R]): Source[(L, Option[R]), NotUsed] =
      StreamJoin[O, LKT, RKT, L, R, (L, Option[R])](
        leftSource.source,
        rightSource.source,
        Join(leftSource.key, rightSource.key, LeftOuter),
        zipper = {
          case (Some(l), r) => (l, r)
        }
      )
  }

  implicit class SortedUniqueStreamOps[O: Ordering, L](leftSource: SortedSource[O, UniqueKey.type, L]) {
    def fullJoin[R](rightSource: SortedSource[O, UniqueKey.type, R]): Source[(Option[L], Option[R]), NotUsed] =
      StreamJoin(leftSource.source, rightSource.source)(
        Join(leftSource.key, rightSource.key, Full)
      )
  }

  implicit def joinKeyOrdering[O: Ordering]: Ordering[JoinKey[O, KeyType]] =
    Ordering.by[JoinKey[O, KeyType], O](_.value)

  case class JoinKey[O: Ordering, KT <: KeyType](value: O, keyType: KT)

  sealed trait KeyType
  case object UniqueKey    extends KeyType
  case object NonUniqueKey extends KeyType

  def uniqueKey[O: Ordering](value: O): JoinKey[O, UniqueKey.type] =
    JoinKey(value, UniqueKey)

  def nonUniqueKey[O: Ordering](value: O): JoinKey[O, NonUniqueKey.type] =
    JoinKey(value, NonUniqueKey)

  sealed trait JoinType
  case object Inner     extends JoinType
  case object LeftOuter extends JoinType
  case object Full      extends JoinType

  case class Join[O: Ordering, LKT <: KeyType, RKT <: KeyType, L, R](
      leftKey: L => JoinKey[O, LKT],
      rightKey: R => JoinKey[O, RKT],
      joinType: JoinType
  ) {
    def leftKeyType(l: L): KeyType =
      leftKey(l).keyType

    def rightKeyType(r: R): KeyType =
      rightKey(r).keyType
  }

  case class SortedSource[O: Ordering, KT <: KeyType, E](source: Source[E, NotUsed], key: E => JoinKey[O, KT])

  def apply[O: Ordering, LKT <: KeyType, RKT <: KeyType, L, R](
      leftSource: Source[L, NotUsed],
      rightSource: Source[R, NotUsed]
  )(
      join: Join[O, LKT, RKT, L, R]
  ): Source[(Option[L], Option[R]), NotUsed] =
    apply(
      leftSource,
      rightSource,
      join,
      zipper = new PartialFunction[(Option[L], Option[R]), (Option[L], Option[R])] {
        def apply(v1: (Option[L], Option[R])): (Option[L], Option[R]) = v1

        def isDefinedAt(x: (Option[L], Option[R])): Boolean = true
      }
    )

  def apply[O: Ordering, LKT <: KeyType, RKT <: KeyType, L, R, LR](
      leftSource: Source[L, NotUsed],
      rightSource: Source[R, NotUsed],
      join: Join[O, LKT, RKT, L, R],
      zipper: PartialFunction[(Option[L], Option[R]), LR]
  ): Source[LR, NotUsed] = {
    Source.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val m = builder.add(new StreamJoin[O, LKT, RKT, L, R, LR](zipper, join))

      leftSource ~> m.in0
      rightSource ~> m.in1

      SourceShape(m.out)
    })
  }

}
