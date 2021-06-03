package sangria.execution

import sangria.marshalling.ResultMarshaller
import sangria.ast
import sangria.schema.ObjectType

case class ExecutionPath private (
  path: List[Any],
  cacheKeyPath: ExecutionPath.PathCacheKey,
  private val cacheKeyPathSize: Int) {
  def add(field: ast.Field, parentType: ObjectType[_, _]): ExecutionPath =
    copy(
      field.outputName :: path,
      field.outputName :: parentType.name :: cacheKeyPath,
      cacheKeyPathSize + 2)

  def withIndex(idx: Int) = new ExecutionPath(idx :: path, cacheKey, cacheKeyPathSize + 1)

  def isEmpty = path.isEmpty
  def nonEmpty = path.nonEmpty

  /** @return last index in the path, if available
   */
  def lastIndex: Option[Int] = path.headOption.collect { case i: Int => i }

  /** @return the size of the path excluding the indexes
   */
  def size = cacheKeyPathSize / 2

  // JMB TODO: change marshalling arrayNode to accept any old Seq
  def marshal(m: ResultMarshaller): m.Node = m.arrayNode(path.toVector.reverse.map {
    case s: String => m.scalarNode(s, "String", Set.empty)
    case i: Int => m.scalarNode(i, "Int", Set.empty)
  })

  def cacheKey: ExecutionPath.PathCacheKey = cacheKeyPath

  override def toString = path.foldLeft("") {
    case ("", str: String) => str
    case (acc, str: String) => acc + "." + str
    case (acc, idx: Int) => acc + "[" + idx + "]"

    case ("", other) => other.toString
    case (acc, other) => acc + "." + other.toString
  }
}

object ExecutionPath {
  type PathCacheKey = List[String]

  val empty = new ExecutionPath(Nil, Nil, 0)
}
