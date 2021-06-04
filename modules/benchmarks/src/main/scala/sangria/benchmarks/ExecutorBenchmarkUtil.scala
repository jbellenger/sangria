package sangria.benchmarks

import sangria.ast.Document
import sangria.execution.Executor
import sangria.execution.deferred.{Deferred, DeferredResolver}
import sangria.parser.QueryParser
import sangria.schema.{Action, Context, Field, ObjectType, OutputType, Schema, StringType}
import sangria.validation.QueryValidator
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

class ExecutorBenchmarkUtil(breadth: Int, depth: Int) {
  import ExecutorBenchmarkUtil._

  def mkSchema(leafResolve: Resolve): Schema[Unit, Any] = {
    lazy val branch: ObjectType[Unit, Any] = ObjectType[Unit, Any](
      "Branch",
      () => {
        val bf = mkField("branch", branch, ctx => ())
        val leaves =
          (1 to breadth).map(i => mkField(leafFieldName(i), StringType, leafResolve)).toList
        bf :: leaves
      }
    )

    Schema(
      branch.rename("Query")
    )
  }

  lazy val query: Document = {
    @tailrec
    def loop(acc: StringBuilder, d: Int): String =
      d match {
        case 0 => acc.toString
        case x =>
          val leafSelectionSet = (1 to breadth).map(i => leafFieldName(i)).mkString(" ")
          loop(
            acc.append(s" branch { $leafSelectionSet "),
            x - 1
          )
      }

    val q = "query { " + loop(new StringBuilder, depth) + (" }" * depth) + "}"
    QueryParser.parse(q).get
  }

  def mkExecutor(leafResolve: Resolve): Executor[Unit, Any] = {
    Executor(
      mkSchema(leafResolve),
      // Executor benchmarks focus on performance of the underlying Resolver.
      // Let's disable query validation for maximum speed.
      queryValidator = QueryValidator.empty,
      deferredResolver = MyDeferredResolver
    )(scala.concurrent.ExecutionContext.global)
  }

  private def leafFieldName(i: Int): String = s"leaf_$i"
  private def mkField(
    name: String,
    fieldType: OutputType[_],
    resolve: Resolve
  ): Field[Unit, Any] =
    new Field(
      name,
      fieldType,
      None,
      Nil,
      resolve,
      None,
      Nil,
      None,
      () => Nil,
      Vector.empty,
      Vector.empty)
}

object ExecutorBenchmarkUtil {

  case class MyDeferred(result: Any) extends Deferred[Any]
  object MyDeferredResolver extends DeferredResolver[Unit] {
    override def resolve(
      deferred: Seq[Deferred[Any]],
      ctx: Unit,
      queryState: Any
    )(
      implicit ec: ExecutionContext
    ): Seq[Future[Any]] = {
      deferred.map {
        case MyDeferred(r) => Future(r)
      }
    }
  }

  type Resolve = Context[Unit, Any] => Action[Unit, Any]
  object Resolve {
    val SingleValue: Resolve = ctx => ctx.field.name
    val SingleDeferred: Resolve = ctx => MyDeferred(ctx.field.name)
  }
}
