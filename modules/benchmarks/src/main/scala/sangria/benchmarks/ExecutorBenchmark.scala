package sangria.benchmarks

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import sangria.ast.Document
import sangria.execution.Executor
import scala.concurrent.Await
import scala.concurrent.duration.Duration

@State(Scope.Thread)
class ExecutorBenchmark {
  @Param(Array("Deferred"))
  var mode: String = _

  @Param(Array("150"))
  var depth: Int = _

  @Param(Array("150"))
  var breadth: Int = _

  private var executor: Executor[Unit, Any] = _
  private var query: Document = _

  @Setup
  def setup(): Unit = {
    val util = new ExecutorBenchmarkUtil(breadth, depth)
    val resolve = mode match {
      case "Value" => ExecutorBenchmarkUtil.Resolve.SingleValue
      case "Deferred" => ExecutorBenchmarkUtil.Resolve.SingleDeferred
    }
    executor = util.mkExecutor(resolve)
    query = util.query
  }

  @Benchmark
  def bench(bh: Blackhole): Unit = {
    val fut = executor.execute(query, (), ())
    val result = Await.result(fut, Duration.Inf)
    bh.consume(result)
  }
}
