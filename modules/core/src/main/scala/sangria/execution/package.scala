package sangria

package object execution {
  val RemovedForSimplification: Error =
    new NotImplementedError("You hit a code path that was removed to support refactoring")
}
