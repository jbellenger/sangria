package sangria.execution

import sangria.ast
import sangria.ast.AstLocation
import sangria.execution.deferred.{Deferred, DeferredResolver}
import sangria.marshalling.{ResultMarshaller, ScalarValueInfo}
import sangria.parser.SourceMapper
import sangria.schema._
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class Resolver[Ctx](
  val marshaller: ResultMarshaller,
  schema: Schema[Ctx, _],
  valueCollector: ValueCollector[Ctx, _],
  variables: Map[String, VariableValue],
  fieldCollector: FieldCollector[Ctx, _],
  userContext: Ctx,
  exceptionHandler: ExceptionHandler,
  deferredResolver: DeferredResolver[Ctx],
  sourceMapper: Option[SourceMapper],
  deprecationTracker: DeprecationTracker,
  maxQueryDepth: Option[Int],
  deferredResolverState: Any,
  preserveOriginalErrors: Boolean,
  queryAst: ast.Document
)(
  implicit executionContext: ExecutionContext) {
  val resultResolver = new ResultResolver(marshaller, exceptionHandler, preserveOriginalErrors)

  import Resolver._
  import resultResolver._

  /**
   * Resolve fields in parallel. Used for query and subscription operations
   */
  def resolveFieldsPar(
    tpe: ObjectType[Ctx, _],
    value: Any,
    fields: CollectedFields
  )(
    scheme: ExecutionScheme
  ): scheme.Result[Ctx, marshaller.Node] = {
    val actions =
      collectActionsPar(ExecutionPath.empty, tpe, value, fields, ErrorRegistry.empty, userContext)

    handleScheme(
      processFinalResolve(
        resolveActionsPar(ExecutionPath.empty, tpe, actions, userContext, fields.namesOrdered))
        .map(_ -> userContext),
      scheme)
  }

  /**
   * Resolve fields sequentially. Used for mutation operations
   */
  def resolveFieldsSeq(
    tpe: ObjectType[Ctx, _],
    value: Any,
    fields: CollectedFields
  )(
    scheme: ExecutionScheme
  ): scheme.Result[Ctx, marshaller.Node] = {
    val result = resolveSeq(ExecutionPath.empty, tpe, value, fields, ErrorRegistry.empty)

    handleScheme(result.flatMap(res => processFinalResolve(res._1).map(_ -> res._2)), scheme)
  }

  /**
   * Apply an ExecutionScheme to the finalResolve value
   */
  private def handleScheme(
    result: Future[((Vector[RegisteredError], marshaller.Node), Ctx)],
    scheme: ExecutionScheme
  ): scheme.Result[Ctx, marshaller.Node] = scheme match {
    case ExecutionScheme.Default =>
      result
        .map {
          case ((_, res), _) => res
        }
        .asInstanceOf[scheme.Result[Ctx, marshaller.Node]]

    case ExecutionScheme.Extended =>
      result
        .map {
          case ((errors, res), uc) => ExecutionResult(uc, res, errors)
        }
        .asInstanceOf[scheme.Result[Ctx, marshaller.Node]]

    case s =>
      throw new IllegalStateException(s"Unsupported execution scheme: $s")
  }

  /**
   * Finalize a Resolve into a Future of errors and results
   */
  private def processFinalResolve(
    resolve: Resolve
  ): Future[(Vector[RegisteredError], marshaller.Node)] = resolve match {
    case Result(errors, data, _) =>
      Future.successful(
        errors.originalErrors ->
          marshalResult(
            data.asInstanceOf[Option[resultResolver.marshaller.Node]],
            marshalErrors(errors),
            None
          ).asInstanceOf[marshaller.Node])

    case dr: DeferredResult =>
      immediatelyResolveDeferred(
        userContext,
        dr,
        _.map {
          case Result(errors, data, _) =>
            errors.originalErrors ->
              marshalResult(
                data.asInstanceOf[Option[resultResolver.marshaller.Node]],
                marshalErrors(errors),
                None
              ).asInstanceOf[marshaller.Node]
        }
      )
  }

  /**
   * Resolve a DeferredResult
   *
   * Deferred's allow for deferring resolution to allow for batching to potential backends.
   * This method *realizes* that deferrment, producing a Future that describes an active call
   * for a group of batched field resolvers.
   */
  private def immediatelyResolveDeferred[T](
    uc: Ctx,
    dr: DeferredResult,
    fn: Future[Result] => Future[T]
  ): Future[T] = {
    val res = fn(dr.futureValue)

    resolveDeferredWithGrouping(dr.deferred).foreach { groups =>
      groups.foreach(group => resolveDeferred(uc, group))
    }

    res
  }

  /**
   * Group a collection of [[Defer]]s. This is a hook that allows a [[DeferredResolver]] to create
   * groupings of Defers that will be batched together. An example of this is to group Defers by
   * complexity in order to execute-and-stream-back cheap defers before starting on more expensive
   * ones.
   * See https://sangria-graphql.github.io/learn/#customizing-deferredresolver-behaviour
   *
   * Note that this method name implies that it resolves the Defers when it doesn't.
   * It only groups them
   */
  private def resolveDeferredWithGrouping(
    deferred: Seq[Future[Array[Defer]]]
  ): Future[Vector[Vector[Defer]]] =
    Future
      .sequence(deferred).map(listOfDef =>
        deferredResolver.groupDeferred(listOfDef.flatten.toVector))

  /**
   * Resolve a selection set sequentially and realize deferments.
   *
   * This primarily supports mutations and is not used in the Query execution path.
   */
  private def resolveSeq(
    path: ExecutionPath,
    tpe: ObjectType[Ctx, _],
    value: Any,
    fields: CollectedFields,
    errorReg: ErrorRegistry
  ): Future[(Result, Ctx)] = {
    fields.fields
      .foldLeft(
        Future.successful(
          (
            Result(ErrorRegistry.empty, Some(marshaller.emptyMapNode(fields.namesOrdered))),
            userContext
          )
        )
      ) {
        case (future, elem) =>
          future.flatMap { resAndCtx =>
            (resAndCtx, elem) match {
              case (acc @ (Result(_, None, _), _), _) => Future.successful(acc)
              case (acc, CollectedField(_, origField, _))
                  if tpe.getField(schema, origField.name).isEmpty =>
                Future.successful(acc)
              case (
                    (Result(errors, Some(acc), _), uc),
                    CollectedField(_, origField, Failure(error))
                  ) =>
                Future.successful(Result(
                  errors.add(path.add(origField, tpe), error),
                  if (isOptional(tpe, origField.name))
                    Some(
                      marshaller.addMapNodeElem(
                        acc.asInstanceOf[marshaller.MapBuilder],
                        origField.outputName,
                        marshaller.nullNode,
                        optional = true))
                  else None
                ) -> uc)
              case (
                    (accRes @ Result(errors, Some(acc), _), uc),
                    CollectedField(name, origField, Success(fields))) =>
                resolveSingleFieldSeq(
                  path,
                  uc,
                  tpe,
                  value,
                  errors,
                  name,
                  origField,
                  fields,
                  accRes,
                  acc
                )
            }
          }
      }
      .map {
        case (res, ctx) => res.buildValue -> ctx
      }
  }

  /**
   * Resolve a single field within a sequential execution.
   *
   * This primarily supports mutations and is not used in the Query execution path.
   */
  private def resolveSingleFieldSeq(
    path: ExecutionPath,
    uc: Ctx,
    tpe: ObjectType[Ctx, _],
    value: Any,
    errors: ErrorRegistry,
    name: String,
    origField: ast.Field,
    fields: Array[ast.Field],
    accRes: Result,
    acc: Any // from `accRes`
  ): Future[(Result, Ctx)] =
    resolveField(uc, tpe, path.add(origField, tpe), value, errors, name, fields) match {
      case ErrorFieldResolution(updatedErrors) if isOptional(tpe, origField.name) =>
        Future.successful(
          Result(
            updatedErrors,
            Some(
              marshaller.addMapNodeElem(
                acc.asInstanceOf[marshaller.MapBuilder],
                fields.head.outputName,
                marshaller.nullNode,
                optional = isOptional(tpe, origField.name)))
          ) -> uc)
      case ErrorFieldResolution(updatedErrors) =>
        Future.successful(Result(updatedErrors, None) -> uc)
      case resolution: StandardFieldResolution =>
        resolveStandardFieldResolutionSeq(
          path,
          uc,
          tpe,
          name,
          origField,
          fields,
          accRes,
          acc,
          resolution)
    }

  /**
   * Resolve a FieldResolution for non-error fields.
   *
   * This primarily supports mutations and is not used in the Query execution path.
   */
  private def resolveStandardFieldResolutionSeq(
    path: ExecutionPath,
    uc: Ctx,
    tpe: ObjectType[Ctx, _],
    name: String,
    origField: ast.Field,
    fields: Array[ast.Field],
    accRes: Result,
    acc: Any, // from `accRes`
    resolution: StandardFieldResolution
  ): Future[(Result, Ctx)] = {
    val StandardFieldResolution(updatedErrors, result, newUc) = resolution
    val sfield = tpe.getField(schema, origField.name).head
    val fieldPath = path.add(fields.head, tpe)

    def resolveUc(v: Any) = newUc.map(_.ctxFn(v)).getOrElse(uc)

    def resolveError(e: Throwable) = {
      try newUc.foreach(_.onError(e))
      catch {
        case NonFatal(ee) => ee.printStackTrace()
      }

      e
    }

    def resolveVal(v: Any) = newUc match {
      case Some(MappedCtxUpdate(_, mapFn, _)) => mapFn(v)
      case None => v
    }

    val resolve =
      try result match {
        case Value(v) =>
          val updatedUc = resolveUc(v)

          Future.successful(
            resolveValue(
              fieldPath,
              fields,
              sfield.fieldType,
              sfield,
              resolveVal(v),
              updatedUc) -> updatedUc)

        case _: MappedSequenceLeafAction[_, _, _] =>
          Future.failed(
            new IllegalStateException("MappedSequenceLeafAction is not supposed to appear here"))
      } catch {
        case NonFatal(e) =>
          Future.successful(
            Result(ErrorRegistry(fieldPath, resolveError(e), fields.head.location), None) -> uc)
      }

    resolve.flatMap {
      case (r: Result, newUc) =>
        Future.successful(
          accRes.addToMap(
            r,
            fields.head.outputName,
            isOptional(tpe, fields.head.name),
            fieldPath,
            fields.head.location,
            updatedErrors) -> newUc)
      case (dr: DeferredResult, newUc) =>
        immediatelyResolveDeferred(
          newUc,
          dr,
          _.map(
            accRes.addToMap(
              _,
              fields.head.outputName,
              isOptional(tpe, fields.head.name),
              fieldPath,
              fields.head.location,
              updatedErrors) -> newUc))
    }
  }

  /**
   * Calculate complexity of a field.
   *
   * You may be wondering: isn't this already done by the [[MeasureComplexity]] query reducer?
   *
   * It is! But where MeasureComplexity calculates a total complexity for a complete query document,
   * This function recalculates that complexity for an individual field level.
   * This complexity score will be included in a [[DeferredWithInfo]] so that a [[DeferredResolver]]
   * may have the option of executing cheap fields before more expensive ones.
   */
  private def calcComplexity(
    path: ExecutionPath,
    astField: ast.Field,
    field: Field[Ctx, _],
    uc: Ctx
  ) = {
    val args = valueCollector.getFieldArgumentValues(
      path,
      Some(astField),
      field.arguments,
      astField.arguments,
      variables)

    args match {
      case Success(a) => a -> field.complexity.fold(DefaultComplexity)(_(uc, a, DefaultComplexity))
      case _ => Args.empty -> DefaultComplexity
    }
  }

  /**
   * Resolve the fields in a CollectedFields and return the Actions of the selection set
   *
   * This is not used for top-level mutation fields. The closest analog to
   * this method for those fields is [[resolveFieldsSeq]]
   */
  private def collectActionsPar(
    path: ExecutionPath,
    tpe: ObjectType[Ctx, _],
    value: Any,
    fields: CollectedFields,
    errorReg: ErrorRegistry,
    userCtx: Ctx
  ): Actions = {
    fields.fields.foldLeft((errorReg, CollectedActions.SomeEmpty): Actions) {
      case (acc @ (_, None), _) => acc
      case (acc, CollectedField(name, origField, _))
          if tpe.getField(schema, origField.name).isEmpty =>
        acc
      case ((errors, s @ Some(acc)), CollectedField(name, origField, Failure(error))) =>
        errors.add(path.add(origField, tpe), error) -> (if (isOptional(tpe, origField.name))
                                                          Some(acc :+ (Array(origField) -> None))
                                                        else None)
      case ((errors, s @ Some(acc)), CollectedField(name, origField, Success(fields))) =>
        resolveField(userCtx, tpe, path.add(origField, tpe), value, errors, name, fields) match {
          case StandardFieldResolution(updatedErrors, result, updateCtx) =>
            updatedErrors -> Some(
              acc :+ (fields -> Some(
                (tpe.getField(schema, origField.name).head, updateCtx, result))))
          case ErrorFieldResolution(updatedErrors) if isOptional(tpe, origField.name) =>
            updatedErrors -> Some(acc :+ (Array(origField) -> None))
          case ErrorFieldResolution(updatedErrors) => updatedErrors -> None
        }
    }
  }

  /**
   * Resolve an ordered set of fields on an object, returning a [[Resolve]] that captures either
   * a synchronous ([[Result]]) or asynchronous ([[DeferredResult]] resolution of these fields.
   *
   * This is used to resolve most ObjectType fields. A notable exception is top-level fields
   * in the mutation schema (which are resolved in [[resolveSeq]]). Objects returned by mutation fields
   * will be resolved in this method.
   */
  private def resolveActionsPar(
    path: ExecutionPath,
    tpe: ObjectType[Ctx, _],
    actions: Actions,
    userCtx: Ctx,
    fieldsNamesOrdered: Array[String]
  ): Resolve = {
    val (errors, res) = actions

    def resolveUc(newUc: Option[MappedCtxUpdate[Ctx, Any, Any]], v: Any): Ctx =
      newUc.map(_.ctxFn(v)).getOrElse(userCtx)

    def resolveError(newUc: Option[MappedCtxUpdate[Ctx, Any, Any]], e: Throwable): Throwable = {
      try {
        newUc.map(_.onError(e))
      } catch {
        case NonFatal(ee) => ee.printStackTrace()
      }

      e
    }

    def resolveVal(newUc: Option[MappedCtxUpdate[Ctx, Any, Any]], v: Any): Any =
      newUc match {
        case Some(MappedCtxUpdate(_, mapFn, _)) => mapFn(v)
        case None => v
      }

    res match {
      case None => Result(errors, None)
      case Some(results) =>
        val resolvedValues = results.map {
          case (astFields, None) => astFields.head -> Result(ErrorRegistry.empty, None)
          case (astFields, Some((field, updateCtx, Value(v)))) =>
            val fieldsPath = path.add(astFields.head, tpe)

            try astFields.head -> resolveValue(
              fieldsPath,
              astFields,
              field.fieldType,
              field,
              resolveVal(updateCtx, v),
              resolveUc(updateCtx, v))
            catch {
              case NonFatal(e) =>
                astFields.head -> Result(
                  ErrorRegistry(fieldsPath, resolveError(updateCtx, e), astFields.head.location),
                  None)
            }

          case (astFields, Some((field, updateCtx, DeferredValue(deferred)))) =>
            val fieldsPath = path.add(astFields.head, tpe)
            val promise = Promise[(ChildDeferredContext, Any, Vector[Throwable])]()
            val (args, complexity) = calcComplexity(fieldsPath, astFields.head, field, userContext)
            val defer = Defer(promise, deferred, complexity, field, astFields, args)

            astFields.head -> DeferredResult(
              Array(Future.successful(Array(defer))),
              promise.future
                .flatMap {
                  case (dctx, v, es) =>
                    val uc = resolveUc(updateCtx, v)

                    es.foreach(resolveError(updateCtx, _))

                    resolveValue(
                      fieldsPath,
                      astFields,
                      field.fieldType,
                      field,
                      resolveVal(updateCtx, v),
                      uc).appendErrors(fieldsPath, es, astFields.head.location) match {
                      case r: Result => dctx.resolveResult(r)
                      case er: DeferredResult => dctx.resolveDeferredResult(uc, er)
                    }
                }
                .recover {
                  case e =>
                    Result(
                      ErrorRegistry(
                        fieldsPath,
                        resolveError(updateCtx, e),
                        astFields.head.location),
                      None)
                }
            )

          case (_, Some((_, _, _))) => throw RemovedForSimplification

        }

        val simpleRes = resolvedValues.collect { case (af, r: Result) => af -> r }

        val resSoFar =
          simpleRes.foldLeft(Result(errors, Some(marshaller.emptyMapNode(fieldsNamesOrdered)))) {
            case (res, (astField, other)) =>
              res.addToMap(
                other,
                astField.outputName,
                isOptional(tpe, astField.name),
                path.add(astField, tpe),
                astField.location,
                res.errors)
          }

        val complexRes = resolvedValues.collect { case (af, r: DeferredResult) => af -> r }

        if (complexRes.isEmpty) resSoFar.buildValue
        else {
          val allDeferred = complexRes.flatMap(_._2.deferred)
          val finalValue = Future
            .sequence(complexRes.toSeq.map {
              case (astField, DeferredResult(_, future)) =>
                future.map(astField -> _)
            })
            .map { results =>
              results
                .foldLeft(resSoFar) {
                  case (res, (astField, other)) =>
                    res.addToMap(
                      other,
                      astField.outputName,
                      isOptional(tpe, astField.name),
                      path.add(astField, tpe),
                      astField.location,
                      res.errors)
                }
                .buildValue
            }

          DeferredResult(allDeferred, finalValue)
        }
    }
  }

  /**
   * Resolve a collection of [[Defer]]s.
   *
   * You're maybe wondering why this doesn't return anything. That's because
   * resolution of Defer's works by mutating a [[Promise]] on each Defer
   */
  private def resolveDeferred(uc: Ctx, toResolve: Vector[Defer]): Unit = {
    if (toResolve.nonEmpty) {
      def findActualDeferred(deferred: Deferred[_]): Deferred[_] = deferred match {
        case MappingDeferred(d, _) => findActualDeferred(d)
        case d => d
      }

      def mapAllDeferred(
        deferred: Deferred[_],
        value: Future[Any]
      ): Future[(Any, Vector[Throwable])] = deferred match {
        case MappingDeferred(d, fn) =>
          val mapFn = fn.asInstanceOf[Any => (Any, Vector[Throwable])]
          mapAllDeferred(d, value)
            .map {
              case (v, errors) =>
                val (mappedV, newErrors) = mapFn(v)
                mappedV -> (errors ++ newErrors)
            }

        case _ => value.map(_ -> Vector.empty)
      }

      try {
        val resolved = deferredResolver.resolve(
          toResolve.map(d => findActualDeferred(d.deferred)),
          uc,
          deferredResolverState)

        if (toResolve.size == resolved.size) {
          val dctx = ParentDeferredContext(uc, toResolve.size)

          for (i <- toResolve.indices) {
            val toRes = toResolve(i)

            toRes.promise.completeWith(
              mapAllDeferred(toRes.deferred, resolved(i))
                .map(v => (dctx.children(i), v._1, v._2))
                .recover {
                  case NonFatal(e) =>
                    dctx.children(i).resolveError(e)
                    throw e
                })
          }

          dctx.init()
        } else {
          toResolve.foreach(_.promise.failure(new IllegalStateException(
            s"Deferred resolver returned ${resolved.size} elements, but it got ${toResolve.size} deferred values. This violates the contract. You can find more information in the documentation: http://sangria-graphql.org/learn/#deferred-values-and-resolver")))
        }
      } catch {
        case NonFatal(error) => toResolve.foreach(_.promise.failure(error))
      }
    }
  }

  /**
   * For a supplied Any field [[value]], fanout, recurse, or otherwise "handle" it based on the
   * field type
   *
   * Notable examples of this include:
   *   - for List types, assume that [[value]] is Seq-like and recursively call this
   *     method with each item in that list
   *
   *   - for Object types:
   *     get a new selection set from [[fieldCollector]],
   *     collect the actions for that selection set ([[collectionActionsPar]],
   *     and resolve the actions ([[resolveActionsPar]]) which will in turn call this method
   *
   * Any [[value]] sent to this method will already have been transformed by any mapping functions
   * included in the field [[Action]]
   */
  private def resolveValue(
    path: ExecutionPath,
    astFields: Array[ast.Field],
    tpe: OutputType[_],
    field: Field[Ctx, _],
    value: Any,
    userCtx: Ctx,
    actualType: Option[InputType[_]] = None
  ): Resolve = {
    tpe match {
      case OptionType(optTpe) =>
        val actualValue = value match {
          case v: Option[_] => v
          case v => Option(v)
        }

        actualValue match {
          case Some(someValue) => resolveValue(path, astFields, optTpe, field, someValue, userCtx)
          case None => Result(ErrorRegistry.empty, None)
        }
      case ListType(listTpe) =>
        if (isUndefinedValue(value))
          Result(ErrorRegistry.empty, None)
        else {
          val actualValue = value match {
            case seq: Seq[_] => seq
            case other => Seq(other)
          }

          val res = actualValue.zipWithIndex.map {
            case (v, idx) =>
              resolveValue(path.withIndex(idx), astFields, listTpe, field, v, userCtx)
          }

          val simpleRes = res.collect { case r: Result => r }
          val optional = isOptional(listTpe)

          // If none of the resolved values in this seq are Deferred, we can call it a "simple" list
          // and resolve the whole thing in a Result
          if (simpleRes.size == res.size) {
            resolveSimpleListValue(simpleRes, path, optional, astFields.head.location)
          } else {
            // this is very hot place, so resorting to mutability to minimize the footprint
            val deferredBuilder = mutable.ArrayBuilder.make[Future[Array[Defer]]]
            val resultFutures = new VectorBuilder[Future[Result]]

            val resIt = res.iterator

            while (resIt.hasNext)
              resIt.next() match {
                case r: Result =>
                  resultFutures += Future.successful(r)
                case dr: DeferredResult =>
                  resultFutures += dr.futureValue
                  deferredBuilder ++= dr.deferred
              }

            DeferredResult(
              deferred = deferredBuilder.result(),
              futureValue = Future
                .sequence(resultFutures.result())
                .map(resolveSimpleListValue(_, path, optional, astFields.head.location))
            )
          }
        }
      case scalar: ScalarType[Any @unchecked] =>
        try Result(
          ErrorRegistry.empty,
          if (isUndefinedValue(value))
            None
          else {
            val coerced = scalar.coerceOutput(value, marshaller.capabilities)

            if (isUndefinedValue(coerced)) {
              None
            } else {
              Some(marshalScalarValue(coerced, marshaller, scalar.name, scalar.scalarInfo))
            }
          }
        )
        catch {
          case NonFatal(e) => Result(ErrorRegistry(path, e), None)
        }
      case scalar: ScalarAlias[Any @unchecked, Any @unchecked] =>
        resolveValue(
          path,
          astFields,
          scalar.aliasFor,
          field,
          scalar.toScalar(value),
          userCtx,
          Some(scalar))
      case enum: EnumType[Any @unchecked] =>
        try {
          Result(
            ErrorRegistry.empty,
            if (isUndefinedValue(value))
              None
            else {
              val coerced = enum.coerceOutput(value)

              if (isUndefinedValue(coerced))
                None
              else
                Some(marshalEnumValue(coerced, marshaller, enum.name))
            }
          )
        } catch {
          case NonFatal(e) => Result(ErrorRegistry(path, e), None)
        }
      case obj: ObjectType[Ctx, _] =>
        if (isUndefinedValue(value))
          Result(ErrorRegistry.empty, None)
        else
          fieldCollector.collectFields(path, obj, astFields) match {
            case Success(fields) =>
              val actions =
                collectActionsPar(path, obj, value, fields, ErrorRegistry.empty, userCtx)

              resolveActionsPar(path, obj, actions, userCtx, fields.namesOrdered)
            case Failure(error) => Result(ErrorRegistry(path, error), None)
          }
      case abst: AbstractType =>
        if (isUndefinedValue(value))
          Result(ErrorRegistry.empty, None)
        else {
          val actualValue =
            abst match {
              case abst: MappedAbstractType[Any @unchecked] => abst.contraMap(value)
              case _ => value
            }

          abst.typeOf(actualValue, schema) match {
            case Some(obj) => resolveValue(path, astFields, obj, field, actualValue, userCtx)
            case None =>
              Result(
                ErrorRegistry(
                  path,
                  UndefinedConcreteTypeError(
                    path,
                    abst,
                    schema.possibleTypes.getOrElse(abst.name, Vector.empty),
                    actualValue,
                    exceptionHandler,
                    sourceMapper,
                    astFields.head.location.toList)
                ),
                None
              )
          }
        }
    }
  }

  private def isUndefinedValue(value: Any) =
    value == null || value == None

  /**
   * A "simple list value" is one where all items have [[Resolve]] values that are
   * synchronous (ie they are [[Result]]), allowing us to synchronously resolve as a Result
   */
  private def resolveSimpleListValue(
    simpleRes: Seq[Result],
    path: ExecutionPath,
    optional: Boolean,
    astPosition: Option[AstLocation]
  ): Result = {
    // this is very hot place, so resorting to mutability to minimize the footprint

    var errorReg = ErrorRegistry.empty
    val listBuilder = new VectorBuilder[marshaller.Node]
    var canceled = false
    val resIt = simpleRes.iterator

    while (resIt.hasNext && !canceled) {
      val res = resIt.next()

      if (!optional && res.value.isEmpty && res.errors.isEmpty)
        errorReg = errorReg.add(path, nullForNotNullTypeError(astPosition))
      else if (res.errors.nonEmpty)
        errorReg = errorReg.add(res.errors)

      res.nodeValue match {
        case node if optional =>
          listBuilder += marshaller.optionalArrayNodeValue(node)
        case Some(other) =>
          listBuilder += other
        case None =>
          canceled = true
      }
    }

    Result(errorReg, if (canceled) None else Some(marshaller.arrayNode(listBuilder.result())))
  }

  private def resolveField(
    userCtx: Ctx,
    tpe: ObjectType[Ctx, _],
    path: ExecutionPath,
    value: Any,
    errors: ErrorRegistry,
    name: String,
    astFields: Array[ast.Field]
  ): FieldResolution = {
    val astField = astFields.head
    val allFields = tpe.getField(schema, astField.name).asInstanceOf[Vector[Field[Ctx, Any]]]
    val field = allFields.head

    maxQueryDepth match {
      case Some(max) if path.size > max =>
        ErrorFieldResolution(errors.add(path, MaxQueryDepthReachedError(max), astField.location))
      case _ =>
        valueCollector.getFieldArgumentValues(
          path,
          Some(astField),
          field.arguments,
          astField.arguments,
          variables) match {
          case Success(args) =>
            val ctx = Context[Ctx, Any](
              value,
              userCtx,
              args,
              schema.asInstanceOf[Schema[Ctx, Any]],
              field,
              tpe.asInstanceOf[ObjectType[Ctx, Any]],
              marshaller,
              queryAst,
              sourceMapper,
              deprecationTracker,
              astFields,
              path,
              deferredResolverState
            )

            if (allFields.exists(_.deprecationReason.isDefined))
              deprecationTracker.deprecatedFieldUsed(ctx)

            try {
              field.resolve(ctx) match {
                case resolved: Value[Ctx, Any @unchecked] =>
                  StandardFieldResolution(errors, resolved, None)

                case resolved: LeafAction[Ctx, Any @unchecked] =>
                  StandardFieldResolution(
                    errors,
                    resolved,
                    None
                  )

                case res: UpdateCtx[Ctx, Any @unchecked] =>
                  StandardFieldResolution(
                    errors,
                    res.action,
                    Some(
                      MappedCtxUpdate(
                        res.nextCtx,
                        identity,
                        _ => ()
                      )
                    )
                  )

                case res: MappedUpdateCtx[Ctx, Any @unchecked, Any @unchecked] =>
                  StandardFieldResolution(
                    errors,
                    res.action,
                    Some(
                      MappedCtxUpdate(
                        res.nextCtx,
                        res.mapFn,
                        _ => ()
                      )
                    )
                  )
              }
            } catch {
              case NonFatal(e) =>
                try {
                  ErrorFieldResolution(errors.add(path, e, astField.location))
                } catch {
                  case NonFatal(me) =>
                    ErrorFieldResolution(
                      errors.add(path, e, astField.location).add(path, me, astField.location)
                    )
                }
            }
          case Failure(error) => ErrorFieldResolution(errors.add(path, error))
        }
    }
  }

  private def isOptional(tpe: ObjectType[_, _], fieldName: String): Boolean =
    isOptional(tpe.getField(schema, fieldName).head.fieldType)

  private def isOptional(tpe: OutputType[_]): Boolean =
    tpe.isInstanceOf[OptionType[_]]

  private def nullForNotNullTypeError(position: Option[AstLocation]) =
    new ExecutionError(
      "Cannot return null for non-nullable type",
      exceptionHandler,
      sourceMapper,
      position.toList)

  /**
   * Describes the outputs of resolving a selection set.
   *
   * Outputs may include errors, [[LeafAction]]'s, and a [[MappedCtxUpdate]] that can produce the
   * context for each fields subtree.
   */
  private type CollectedActions =
    Option[Array[
      (
        Array[ast.Field],
        Option[(Field[Ctx, _], Option[MappedCtxUpdate[Ctx, Any, Any]], LeafAction[Ctx, _])]
      )
    ]]

  private object CollectedActions {
    val SomeEmpty: CollectedActions = Some(Array.empty)
  }

  private type Actions = (ErrorRegistry, CollectedActions)

  private sealed trait CollectedActions2
  private object CollectedActions2 {
    case object None extends CollectedActions2
    case object Empty extends CollectedActions2

    case class ResolvedAction(
      schemaField: Field[Ctx, _],
      update: Option[MappedCtxUpdate[Ctx, Any, Any]],
      action: LeafAction[Ctx, _])
    case class Actions(fields: Array[ast.Field], schemaField: Field[Ctx, _], action: ResolvedAction)
  }

  private case class Actions2(errors: ErrorRegistry, actions: CollectedActions2)

  /**
   * A normalized interface for the outputs of a field resolver
   */
  private sealed trait Resolve {
    def appendErrors(
      path: ExecutionPath,
      errors: Vector[Throwable],
      position: Option[AstLocation]
    ): Resolve
  }

  /**
   * A Deferred Resolve, for describing async outputs of a field resolver (even outputs
   * that are not exactly Defer's)
   *
   * You may ask: what is the difference between deferred and a futureValue? Aren't they the same thing?
   * They are not the same thing. I (JMB) think the difference is:
   *   - `futureValue` supports the FutureValue action type, which is the simplest kind of async
   *     resolver output
   *
   *   - `deferred` supports the DeferredValue and DeferredFutureValue action types, which are
   *     an async resolver output that adds a layer of indirection via DeferredResolver.
   *     Deferreds allows for batching of field resolvers which is not something that is not
   *     easily done using Futures.
   *     More on this at https://sangria-graphql.github.io/learn/#deferred-value-resolution
   */
  private case class DeferredResult(
    deferred: Seq[Future[Array[Defer]]],
    futureValue: Future[Result])
      extends Resolve {
    def appendErrors(
      path: ExecutionPath,
      errors: Vector[Throwable],
      position: Option[AstLocation]
    ) =
      if (errors.nonEmpty)
        copy(futureValue = futureValue.map(_.appendErrors(path, errors, position)))
      else this
  }

  /**
   * An implementation of DeferredWithInfo. Essentially a context wrapper around a Deferred.
   *
   * The process of resolving a Deferred works by fulfilling the [[promise]] on this object.
   * This makes them mutable. See [[resolveDeferred]]
   */
  private case class Defer(
    promise: Promise[(ChildDeferredContext, Any, Vector[Throwable])],
    deferred: Deferred[Any],
    complexity: Double,
    field: Field[_, _],
    astFields: Array[ast.Field],
    args: Args)
      extends DeferredWithInfo

  /**
   * A Resolve that describes synchronous outputs of a field resolver
   */
  private case class Result(
    errors: ErrorRegistry,
    value: Option[Any /* Either marshaller.Node or marshaller.MapBuilder */ ],
    userContext: Option[Ctx] = None)
      extends Resolve {

    def addToMap(
      other: Result,
      key: String,
      optional: Boolean,
      path: ExecutionPath,
      position: Option[AstLocation],
      updatedErrors: ErrorRegistry
    ): Result =
      copy(
        errors =
          if (!optional && other.value.isEmpty && other.errors.isEmpty)
            updatedErrors.add(other.errors).add(path, nullForNotNullTypeError(position))
          else
            updatedErrors.add(other.errors),
        value = if (optional && other.value.isEmpty) {
          value.map(v =>
            marshaller.addMapNodeElem(
              v.asInstanceOf[marshaller.MapBuilder],
              key,
              marshaller.nullNode,
              optional = false
            ))
        } else {
          for {
            myVal <- value
            otherVal <- other.value
          } yield {
            marshaller.addMapNodeElem(
              myVal.asInstanceOf[marshaller.MapBuilder],
              key,
              otherVal.asInstanceOf[marshaller.Node],
              optional = false
            )
          }
        }
      )

    def nodeValue: Option[marshaller.Node] =
      value.asInstanceOf[Option[marshaller.Node]]

    def builderValue: Option[marshaller.MapBuilder] =
      value.asInstanceOf[Option[marshaller.MapBuilder]]

    def buildValue: Result = copy(value = builderValue.map(marshaller.mapNode))

    override def appendErrors(
      path: ExecutionPath,
      e: Vector[Throwable],
      position: Option[AstLocation]
    ): Result = {
      if (e.nonEmpty) copy(errors = errors.append(path, e, position))
      else this
    }
  }

  private case class ParentDeferredContext(uc: Ctx, expectedBranches: Int) {
    val children =
      Seq.fill(expectedBranches)(ChildDeferredContext(Promise[Seq[Future[Array[Defer]]]]()))

    def init(): Unit =
      Future.sequence(children.map(_.promise.future)).onComplete { res =>
        val allDeferred = res.get.flatten

        if (allDeferred.nonEmpty)
          resolveDeferredWithGrouping(allDeferred).foreach(groups =>
            groups.foreach(group => resolveDeferred(uc, group)))
      }
  }

  private case class ChildDeferredContext(promise: Promise[Seq[Future[Array[Defer]]]]) {
    def resolveDeferredResult(uc: Ctx, res: DeferredResult): Future[Result] = {
      promise.success(res.deferred)
      res.futureValue
    }

    def resolveResult(res: Result): Future[Result] = {
      promise.success(Vector.empty)
      Future.successful(res)
    }

    def resolveError(e: Throwable): Unit =
      promise.success(Vector.empty)
  }

  private sealed trait FieldResolution
  private case class ErrorFieldResolution(errors: ErrorRegistry) extends FieldResolution
  private case class StandardFieldResolution(
    errors: ErrorRegistry,
    action: LeafAction[Ctx, Any],
    ctxUpdate: Option[MappedCtxUpdate[Ctx, Any, Any]])
      extends FieldResolution
}

object Resolver {
  val DefaultComplexity = 1.0d

  def marshalEnumValue(
    value: String,
    marshaller: ResultMarshaller,
    typeName: String
  ): marshaller.Node =
    marshaller.enumNode(value, typeName)

  def marshalScalarValue(
    value: Any,
    marshaller: ResultMarshaller,
    typeName: String,
    scalarInfo: Set[ScalarValueInfo]
  ): marshaller.Node =
    value match {
      case astValue: ast.Value => marshalAstValue(astValue, marshaller, typeName, scalarInfo)
      case null => marshaller.nullNode
      case v => marshaller.scalarNode(value, typeName, scalarInfo)
    }

  def marshalAstValue(
    value: ast.Value,
    marshaller: ResultMarshaller,
    typeName: String,
    scalarInfo: Set[ScalarValueInfo]
  ): marshaller.Node = value match {
    case ast.StringValue(str, _, _, _, _) => marshaller.scalarNode(str, typeName, scalarInfo)
    case ast.IntValue(i, _, _) => marshaller.scalarNode(i, typeName, scalarInfo)
    case ast.BigIntValue(i, _, _) => marshaller.scalarNode(i, typeName, scalarInfo)
    case ast.FloatValue(f, _, _) => marshaller.scalarNode(f, typeName, scalarInfo)
    case ast.BigDecimalValue(f, _, _) => marshaller.scalarNode(f, typeName, scalarInfo)
    case ast.BooleanValue(b, _, _) => marshaller.scalarNode(b, typeName, scalarInfo)
    case ast.NullValue(_, _) => marshaller.nullNode
    case ast.EnumValue(enum, _, _) => marshaller.enumNode(enum, typeName)
    case ast.ListValue(values, _, _) =>
      marshaller.arrayNode(values.map(marshalAstValue(_, marshaller, typeName, scalarInfo)))
    case ast.ObjectValue(values, _, _) =>
      marshaller.mapNode(
        values.map(v => v.name -> marshalAstValue(v.value, marshaller, typeName, scalarInfo)))
    case ast.VariableValue(name, _, _) => marshaller.enumNode(name, typeName)
  }

  /**
   * A utility class for describing how the value and/or context should be modified
   * after resolving a Field's subtree.
   *
   * This should not be confused with the similarly named [[MappedUpdateCtx]], which is a resolver
   * Action that has actual values for next context and next value.
   *
   * This class does not contain actual context or values, just functions that can transform these
   * things for a fields subtree
   */
  private case class MappedCtxUpdate[Ctx, Val, NewVal](
    ctxFn: Val => Ctx,
    mapFn: Val => NewVal,
    onError: Throwable => Unit)
}

/**
 * An async Deferred, wrapped in some context of where it was encountered during resolution
 */
trait DeferredWithInfo {
  def deferred: Deferred[Any]
  def complexity: Double
  def field: Field[_, _]
  def astFields: Array[ast.Field]
  def args: Args
}
