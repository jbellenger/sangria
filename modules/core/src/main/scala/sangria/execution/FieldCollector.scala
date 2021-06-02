package sangria.execution

import sangria.ast.OperationType
import sangria.parser.SourceMapper
import sangria.schema._
import sangria.ast
import sangria.util.Cache

import scala.collection.mutable.{ArrayBuffer, Map => MutableMap, Set => MutableSet}
import com.twitter.util.{Throw, Return, Try}

class FieldCollector[Ctx, Val](
    schema: Schema[Ctx, Val],
    document: ast.Document,
    variables: Map[String, VariableValue],
    sourceMapper: Option[SourceMapper],
    valueCollector: ValueCollector[Ctx, _],
    exceptionHandler: ExceptionHandler) {

  private val resultCache = Cache.empty[(ExecutionPath.PathCacheKey, String), Try[CollectedFields]]

  def collectFields(
      path: ExecutionPath,
      tpe: ObjectType[Ctx, _],
      selections: Vector[ast.SelectionContainer]): Try[CollectedFields] =
    resultCache.getOrElseUpdate(
      path.cacheKey -> tpe.name, {
        val builder: Try[CollectedFieldsBuilder] = Return(new CollectedFieldsBuilder)

        selections.foldLeft(builder) { case (acc, s) =>
          collectFieldsInternal(tpe, s.selections, MutableSet.empty, acc)
        }

        builder.map(_.build)
      }
    )

  private def collectFieldsInternal(
      tpe: ObjectType[Ctx, _],
      selections: Vector[ast.Selection],
      visitedFragments: MutableSet[String],
      initial: Try[CollectedFieldsBuilder]): Try[CollectedFieldsBuilder] =
    selections.foldLeft(initial) {
      case (f @ Throw(_), selection) => f
      case (s @ Return(acc), selection) =>
        selection match {
          case field @ ast.Field(_, _, _, dirs, _, _, _, _) =>
            val name = field.outputName

            shouldIncludeNode(dirs, selection) match {
              case Return(true) =>
                acc.add(name, field)
                s
              case Return(false) => s
              case Throw(error) =>
                acc.addError(name, field, error)
                s
            }
          case fragment @ ast.InlineFragment(_, dirs, fragmentSelections, _, _, _) =>
            for {
              shouldInclude <- shouldIncludeNode(dirs, selection)
              fragmentConditionMatch <- doesFragmentConditionMatch(tpe, fragment)
              fragmentFields <-
                if (shouldInclude && fragmentConditionMatch)
                  collectFieldsInternal(tpe, fragmentSelections, visitedFragments, s)
                else s
            } yield fragmentFields
          case ast.FragmentSpread(name, _, _, _) if visitedFragments contains name => s
          case ast.FragmentSpread(name, dirs, _, position) =>
            shouldIncludeNode(dirs, selection).flatMap { shouldInclude =>
              if (shouldInclude) {
                visitedFragments += name

                document.fragments.get(name) match {
                  case Some(fragment) =>
                    for {
                      shouldInclude <- shouldIncludeNode(fragment.directives, fragment)
                      fragmentConditionMatch <- doesFragmentConditionMatch(tpe, fragment)
                      fragmentFields <-
                        if (shouldInclude && fragmentConditionMatch)
                          collectFieldsInternal(tpe, fragment.selections, visitedFragments, s)
                        else s
                    } yield fragmentFields
                  case None =>
                    Throw(
                      new ExecutionError(
                        s"Fragment with name '$name' is not defined",
                        exceptionHandler,
                        sourceMapper,
                        position.toList))
                }
              } else s
            }
        }
    }

  def shouldIncludeNode(
      directives: Vector[ast.Directive],
      selection: ast.WithDirectives): Try[Boolean] = {
    val possibleDirs = directives
      .map(d =>
        schema.directivesByName
          .get(d.name)
          .map(dd =>
            selection match {
              case _: ast.Field if !dd.locations.contains(DirectiveLocation.Field) =>
                Throw(
                  new ExecutionError(
                    s"Directive '${dd.name}' is not allowed to be used on fields",
                    exceptionHandler,
                    sourceMapper,
                    d.location.toList))
              case _: ast.InlineFragment
                  if !dd.locations.contains(DirectiveLocation.InlineFragment) =>
                Throw(
                  new ExecutionError(
                    s"Directive '${dd.name}' is not allowed to be used on inline fragment",
                    exceptionHandler,
                    sourceMapper,
                    d.location.toList))
              case _: ast.FragmentSpread
                  if !dd.locations.contains(DirectiveLocation.FragmentSpread) =>
                Throw(
                  new ExecutionError(
                    s"Directive '${dd.name}' is not allowed to be used on fragment spread",
                    exceptionHandler,
                    sourceMapper,
                    d.location.toList))
              case _: ast.FragmentDefinition
                  if !dd.locations.contains(DirectiveLocation.FragmentDefinition) =>
                Throw(
                  new ExecutionError(
                    s"Directive '${dd.name}' is not allowed to be used on fragment definition",
                    exceptionHandler,
                    sourceMapper,
                    d.location.toList))
              case op: ast.OperationDefinition
                  if op.operationType == OperationType.Query && !dd.locations.contains(
                    DirectiveLocation.Query) =>
                Throw(
                  new ExecutionError(
                    s"Directive '${dd.name}' is not allowed to be used on query operation",
                    exceptionHandler,
                    sourceMapper,
                    d.location.toList))
              case op: ast.OperationDefinition
                  if op.operationType == OperationType.Mutation && !dd.locations.contains(
                    DirectiveLocation.Mutation) =>
                Throw(
                  new ExecutionError(
                    s"Directive '${dd.name}' is not allowed to be used on mutation operation",
                    exceptionHandler,
                    sourceMapper,
                    d.location.toList))
              case op: ast.OperationDefinition
                  if op.operationType == OperationType.Subscription && !dd.locations.contains(
                    DirectiveLocation.Subscription) =>
                Throw(
                  new ExecutionError(
                    s"Directive '${dd.name}' is not allowed to be used on subscription operation",
                    exceptionHandler,
                    sourceMapper,
                    d.location.toList))
              case _ => Return(d -> dd)
            })
          .getOrElse(
            Throw(
              new ExecutionError(
                s"Directive '${d.name}' not found.",
                exceptionHandler,
                sourceMapper,
                d.location.toList))))
      .map(_.flatMap { case (astDir, dir) =>
        valueCollector
          .getArgumentValues(Some(astDir), dir.arguments, astDir.arguments, variables)
          .map(dir -> _)
      })

    possibleDirs.collect { case Throw(error) => error }.headOption.map(Throw(_)).getOrElse {
      val validDirs = possibleDirs.collect { case Return(v) => v }
      val should = validDirs.forall { case (dir, args) =>
        dir.shouldInclude(DirectiveContext(selection, dir, args))
      }

      Return(should)
    }
  }

  def doesFragmentConditionMatch(
      tpe: ObjectType[_, _],
      conditional: ast.ConditionalFragment): Try[Boolean] =
    conditional.typeConditionOpt match {
      case Some(tc) =>
        schema.outputTypes
          .get(tc.name)
          .map(condTpe =>
            Return(
              condTpe.name == tpe.name || (condTpe
                .isInstanceOf[AbstractType] && schema.isPossibleType(condTpe.name, tpe))))
          .getOrElse(
            Throw(
              new ExecutionError(
                s"Unknown type '${tc.name}'.",
                exceptionHandler,
                sourceMapper,
                conditional.location.toList)))
      case None => Return(true)
    }
}

case class CollectedFields(namesOrdered: Vector[String], fields: Vector[CollectedField])
case class CollectedField(name: String, field: ast.Field, allFields: Try[Vector[ast.Field]])

// Imperative builder to minimize intermediate object creation
class CollectedFieldsBuilder {
  private val indexLookup = MutableMap[String, Int]()
  private val names = ArrayBuffer[String]()
  private val firstFields = ArrayBuffer[ast.Field]()
  private val fields = ArrayBuffer[Try[ArrayBuffer[ast.Field]]]()

  def contains(name: String) = indexLookup contains name
  def add(name: String, field: ast.Field) = {
    indexLookup.get(name) match {
      case Some(idx) =>
        fields(idx) match {
          case s @ Return(list) => list += field
          case _ => // do nothing because there is already an error
        }
      case None =>
        indexLookup(name) = fields.size
        firstFields += field
        names += name
        fields += Return(ArrayBuffer(field))
    }

    this
  }

  def addError(name: String, field: ast.Field, error: Throwable) = {
    indexLookup.get(name) match {
      case Some(idx) =>
        fields(idx) match {
          case s @ Return(list) =>
            fields(idx) = Throw(error)
          case _ => // do nothing because there is already an error
        }
      case None =>
        indexLookup(name) = fields.size
        firstFields += field
        names += name
        fields += Throw(error)
    }

    this
  }

  def build = {
    val builtFields = firstFields.toVector.zipWithIndex.map { case (f, idx) =>
      CollectedField(names(idx), f, fields(idx).map(_.toVector))
    }

    CollectedFields(names.toVector, builtFields)
  }
}
