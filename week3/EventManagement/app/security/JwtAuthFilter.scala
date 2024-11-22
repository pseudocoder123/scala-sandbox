package security

import org.apache.pekko.stream.Materializer
import play.api.http.HttpFilters
import play.api.libs.typedmap.TypedKey
import play.api.mvc._
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}

object Attrs {
  val UserId: TypedKey[String] = TypedKey[String]("userId")
}

// Filter for JWT Authentication
class JwtAuthFilter @Inject()(
                               implicit val mat: Materializer,
                               ec: ExecutionContext
                             ) extends Filter {



  override def apply(nextFilter: RequestHeader => Future[Result])(request: RequestHeader): Future[Result] = {
    val publicRoutes = Seq("/api/login", "/user")

    if (publicRoutes.exists(request.path.startsWith) || request.path == "/") {
      // Allow public routes without authentication
      nextFilter(request)
    } else {

      val tokenOpt = request.headers.get("Authorization").map(_.replace("Bearer ", ""))
      println(tokenOpt)
      val tokenOptResult =JwtUtil.validateToken(tokenOpt.getOrElse(""))
      println(tokenOptResult)

      // Validate the token and extract the userId
      val userIdOpt = JwtUtil.validateToken(tokenOpt.getOrElse(""))
      println(s"Validated userId: $userIdOpt")

      userIdOpt match {
        case Some(userId) =>
          // Valid token: Proceed with the request
          // Optionally, attach userId to the request context if needed
          val updatedRequest = request.addAttr(Attrs.UserId, userId)
          nextFilter(updatedRequest)
        case None =>
          // Invalid or missing token: Return Unauthorized
          Future.successful(Results.Unauthorized("Invalid or missing token"))
      }
    }
  }
}

// Filters Registration
class Filters @Inject()(jwtAuthFilter: JwtAuthFilter) extends HttpFilters {
  override def filters: Seq[EssentialFilter] = Seq(jwtAuthFilter)
}