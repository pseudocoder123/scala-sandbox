package security

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.exceptions.JWTVerificationException
import java.util.Date
import config.EnvConfig

object JwtUtil {
  private val secretKey = EnvConfig.getJwtUtilSecretKey
  private val algorithm = Algorithm.HMAC256(secretKey)
  private val issuer = EnvConfig.getJwtUtilIssuer

  // Generate a JWT token
  def generateToken(userId: String, expirationMillis: Long = 3600000): String = {
    val now = System.currentTimeMillis()
    JWT.create()
      .withIssuer(issuer)
      .withSubject(userId)
      .withIssuedAt(new Date(now))
      .withExpiresAt(new Date(now + expirationMillis))
      .sign(algorithm)
  }

  // Validate a JWT token
  def validateToken(token: String): Option[String] = {
    try {
      val verifier = JWT.require(algorithm).withIssuer(issuer).build()
      val decodedJWT = verifier.verify(token)
      print(decodedJWT.getSubject)
      Some(decodedJWT.getSubject) // Extract the userId from the token
    } catch {
      case _: JWTVerificationException => None
    }
  }
}