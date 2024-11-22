package services

import models.entity.User
import repositories.UserRepository

import java.security.MessageDigest
import java.util.Base64
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class UserService @Inject()(userRepository: UserRepository)(implicit executionContext: ExecutionContext) {
  // Hash password using SHA-256
  def hashPassword(password: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hashedBytes = digest.digest(password.getBytes("UTF-8"))

    Base64.getEncoder.encodeToString(hashedBytes)
  }

  // Verify if provided password matches stored hash
  def verifyPassword(providedPassword: String, storedHash: String): Boolean = {
    val hashedProvidedPassword = hashPassword(providedPassword)
    hashedProvidedPassword == storedHash
  }

  // Create new user with hashed password
  def create(user: User): Future[Long] = {
    userRepository.getUserByName(user.name).flatMap {
      case Some(_) => Future.failed(new IllegalStateException(s"User name already exists"))
      case None => {
        val hashedUser = user.copy(password = hashPassword(user.password))
        println(hashedUser)
        userRepository.create(hashedUser)
      }
    }
  }

  // Verify user login
  def verifyUser(userName: String, password: String): Future[Boolean] = {
    userRepository.getUserByName(userName).map {
      case Some(user) => verifyPassword(password, user.password)
      case None => false
    }
  }
}
