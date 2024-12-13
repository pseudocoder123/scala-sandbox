package caseStudy1.apiController

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import org.apache.spark.sql.SparkSession
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}
import caseStudy1.config.Config.DataPaths
import caseStudy1.apiController.MetricsControllerServiceImpl.{fetchAggregatedMetrics, fetchAggregatedMetricsBySensorId}
import scala.concurrent.Future

object MetricsController {
  implicit val system = ActorSystem("SensorMetricsAPISystem")
  implicit val materializer: Materializer = Materializer(system)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val spark = SparkSession.builder()
    .appName("AggregatedDataApi")
    .config("spark.hadoop.fs.defaultFS", DataPaths.gsDefaultFS)
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", DataPaths.gsServiceAccJsonPath)
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val route: Route = concat(
      path("api" / "aggregated-data") {
        get {
          onComplete(Future {
            fetchAggregatedMetrics(spark)
          }) {
            case Success(result) =>
              complete(HttpEntity(ContentTypes.`application/json`, result))
            case Failure(exception) =>
              complete(HttpResponse(
                status = StatusCodes.InternalServerError,
                entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, exception.getMessage)
              ))
          }
        }
      },
      path("api" / "aggregated-data" / Segment) { sensorId =>
        get {
          onComplete(Future {
            fetchAggregatedMetricsBySensorId(spark, sensorId.toInt)
          }) {
            case Success(result) =>
              complete(HttpEntity(ContentTypes.`application/json`, result))
            case Failure(exception) =>
              complete(HttpResponse(
                status = StatusCodes.InternalServerError,
                entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, exception.getMessage)
              ))
          }
        }
      }
    )

    Http().newServerAt("0.0.0.0", 8080).bind(route)
    println("Server online at http://0.0.0.0:8080/")
  }
}
