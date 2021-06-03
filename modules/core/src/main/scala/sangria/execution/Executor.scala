package sangria.execution

import sangria.ast
import sangria.execution.Executor.ExceptionHandler
import sangria.execution.deferred.DeferredResolver
import sangria.marshalling.InputUnmarshaller.emptyMapVars
import sangria.marshalling.{InputUnmarshaller, ResultMarshaller}
import sangria.parser.SourceMapper
import sangria.schema._
import sangria.validation.QueryValidator
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

case class Executor[Ctx, Root](
  schema: Schema[Ctx, Root],
  queryValidator: QueryValidator = QueryValidator.default,
  deferredResolver: DeferredResolver[Ctx] = DeferredResolver.empty,
  exceptionHandler: ExceptionHandler = ExceptionHandler.empty,
  deprecationTracker: DeprecationTracker = DeprecationTracker.empty,
  maxQueryDepth: Option[Int] = None
)(
  implicit executionContext: ExecutionContext) {

  def execute[Input](
    queryAst: ast.Document,
    userContext: Ctx,
    root: Root,
    operationName: Option[String] = None,
    variables: Input = emptyMapVars
  )(
    implicit
    marshaller: ResultMarshaller,
    um: InputUnmarshaller[Input],
    scheme: ExecutionScheme
  ): scheme.Result[Ctx, marshaller.Node] = {
    val violations = queryValidator.validateQuery(schema, queryAst)

    if (violations.nonEmpty)
      scheme.failed(ValidationError(violations, exceptionHandler))
    else {
      val scalarMiddleware = None
      val valueCollector = new ValueCollector[Ctx, Input](
        schema,
        variables,
        queryAst.sourceMapper,
        deprecationTracker,
        userContext,
        exceptionHandler,
        scalarMiddleware,
        false)(um)

      val executionResult = for {
        operation <- Executor.getOperation(exceptionHandler, queryAst, operationName)
        unmarshalledVariables <-
          valueCollector.getVariableValues(operation.variables, scalarMiddleware)
        fieldCollector = new FieldCollector[Ctx, Root](
          schema,
          queryAst,
          unmarshalledVariables,
          queryAst.sourceMapper,
          valueCollector,
          exceptionHandler)
        tpe <-
          Executor.getOperationRootType(schema, exceptionHandler, operation, queryAst.sourceMapper)
        fields <- fieldCollector.collectFields(ExecutionPath.empty, tpe, Array(operation))
      } yield {
        val reduced = Future(userContext)
        scheme.flatMapFuture(reduced) { newCtx =>
          executeOperation(
            queryAst,
            operationName,
            variables,
            um,
            operation,
            queryAst.sourceMapper,
            valueCollector,
            fieldCollector,
            marshaller,
            unmarshalledVariables,
            tpe,
            fields,
            newCtx,
            root,
            scheme
          )
        }
      }

      executionResult match {
        case Success(result) => result
        case Failure(error) => scheme.failed(error)
      }
    }
  }

  private def executeOperation[Input](
    queryAst: ast.Document,
    operationName: Option[String],
    inputVariables: Input,
    inputUnmarshaller: InputUnmarshaller[Input],
    operation: ast.OperationDefinition,
    sourceMapper: Option[SourceMapper],
    valueCollector: ValueCollector[Ctx, _],
    fieldCollector: FieldCollector[Ctx, Root],
    marshaller: ResultMarshaller,
    variables: Map[String, VariableValue],
    tpe: ObjectType[Ctx, Root],
    fields: CollectedFields,
    ctx: Ctx,
    root: Root,
    scheme: ExecutionScheme
  ): scheme.Result[Ctx, marshaller.Node] =
    try {
      val deferredResolverState = deferredResolver.initialQueryState

      val resolver = new Resolver[Ctx](
        marshaller,
        schema,
        valueCollector,
        variables,
        fieldCollector,
        ctx,
        exceptionHandler,
        deferredResolver,
        sourceMapper,
        deprecationTracker,
        maxQueryDepth,
        deferredResolverState,
        scheme.extended,
        queryAst)

      operation.operationType match {
        case ast.OperationType.Query =>
          resolver
            .resolveFieldsPar(tpe, root, fields)(scheme)
            .asInstanceOf[scheme.Result[Ctx, marshaller.Node]]
        case ast.OperationType.Mutation =>
          resolver
            .resolveFieldsSeq(tpe, root, fields)(scheme)
            .asInstanceOf[scheme.Result[Ctx, marshaller.Node]]

        case _ => throw RemovedForSimplification
      }
    } catch {
      case NonFatal(error) =>
        scheme.failed(error)
    }
}

object Executor {
  type ExceptionHandler = sangria.execution.ExceptionHandler

  def execute[Ctx, Root, Input](
    schema: Schema[Ctx, Root],
    queryAst: ast.Document,
    userContext: Ctx = (),
    root: Root = (),
    operationName: Option[String] = None,
    variables: Input = emptyMapVars,
    queryValidator: QueryValidator = QueryValidator.default,
    deferredResolver: DeferredResolver[Ctx] = DeferredResolver.empty,
    exceptionHandler: ExceptionHandler = ExceptionHandler.empty,
    deprecationTracker: DeprecationTracker = DeprecationTracker.empty,
    maxQueryDepth: Option[Int] = None
  )(
    implicit
    executionContext: ExecutionContext,
    marshaller: ResultMarshaller,
    um: InputUnmarshaller[Input],
    scheme: ExecutionScheme
  ): scheme.Result[Ctx, marshaller.Node] =
    Executor(
      schema,
      queryValidator,
      deferredResolver,
      exceptionHandler,
      deprecationTracker,
      maxQueryDepth
    ).execute(queryAst, userContext, root, operationName, variables)
      .asInstanceOf[scheme.Result[Ctx, marshaller.Node]]

  def getOperationRootType[Ctx, Root](
    schema: Schema[Ctx, Root],
    exceptionHandler: ExceptionHandler,
    operation: ast.OperationDefinition,
    sourceMapper: Option[SourceMapper]
  ) = operation.operationType match {
    case ast.OperationType.Query =>
      Success(schema.query)
    case ast.OperationType.Mutation =>
      schema.mutation
        .map(Success(_))
        .getOrElse(
          Failure(
            OperationSelectionError(
              "Schema is not configured for mutations",
              exceptionHandler,
              sourceMapper,
              operation.location.toList)))
    case ast.OperationType.Subscription =>
      schema.subscription
        .map(Success(_))
        .getOrElse(
          Failure(
            OperationSelectionError(
              "Schema is not configured for subscriptions",
              exceptionHandler,
              sourceMapper,
              operation.location.toList)))
  }

  def getOperation(
    exceptionHandler: ExceptionHandler,
    document: ast.Document,
    operationName: Option[String]
  ): Try[ast.OperationDefinition] =
    if (document.operations.size != 1 && operationName.isEmpty)
      Failure(
        OperationSelectionError(
          "Must provide operation name if query contains multiple operations",
          exceptionHandler))
    else {
      val unexpectedDefinition = document.definitions.find(d =>
        !(d.isInstanceOf[ast.OperationDefinition] || d.isInstanceOf[ast.FragmentDefinition]))

      unexpectedDefinition match {
        case Some(unexpected) =>
          Failure(new ExecutionError(
            s"GraphQL cannot execute a request containing a ${unexpected.getClass.getSimpleName}.",
            exceptionHandler))
        case None =>
          operationName match {
            case Some(opName) =>
              document.operations
                .get(Some(opName))
                .map(Success(_))
                .getOrElse(Failure(
                  OperationSelectionError(s"Unknown operation name '$opName'", exceptionHandler)))
            case None =>
              Success(document.operations.values.head)
          }
      }
    }

}
