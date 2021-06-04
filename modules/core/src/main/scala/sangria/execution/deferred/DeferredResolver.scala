package sangria.execution.deferred

import sangria.ast
import sangria.execution.DeferredWithInfo
import sangria.schema.{Args, Field}

import scala.concurrent.{ExecutionContext, Future}

trait DeferredResolver[-Ctx] {
  def includeDeferredFromField: Option[(Field[_, _], Vector[ast.Field], Args, Double) => Boolean] =
    None

  def groupDeferred[T <: DeferredWithInfo](deferred: Seq[T]): Seq[Seq[T]] =
    Seq(deferred)

  def initialQueryState: Any = ()

  def resolve(
    deferred: Seq[Deferred[Any]],
    ctx: Ctx,
    queryState: Any
  )(
    implicit ec: ExecutionContext
  ): Seq[Future[Any]]
}

object DeferredResolver {
  val empty = new DeferredResolver[Any] {
    override def resolve(
      deferred: Seq[Deferred[Any]],
      ctx: Any,
      queryState: Any
    )(
      implicit ec: ExecutionContext
    ) =
      deferred.map(d => Future.failed(UnsupportedDeferError(d)))
  }
}

trait Deferred[+T]

case class UnsupportedDeferError(deferred: Deferred[Any])
    extends Exception(s"Deferred resolver is not defined for deferred value: $deferred.")
