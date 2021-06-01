package sangria.execution

import language.{higherKinds, implicitConversions}
import sangria.marshalling.InputUnmarshaller

import sangria.ast
import sangria.schema.{Action, Context, InputType}
import sangria.streaming.SubscriptionStream
import sangria.validation.Violation

trait MiddlewareAttachment

trait FieldTag

case class StringTag(name: String)

object FieldTag {
  implicit def stringTag(s: String): StringTag = StringTag(s)
  implicit def symbolTag(s: Symbol): StringTag = StringTag(s.name)
}

case class SubscriptionField[S[_]](stream: SubscriptionStream[S]) extends FieldTag

case class Extension[In](data: In)(implicit val iu: InputUnmarshaller[In])
