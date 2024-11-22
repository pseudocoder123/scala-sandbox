package utils

import config.EnvConfig
import java.util.Properties
import javax.mail._
import javax.mail.internet._
import javax.mail.internet.InternetAddress

object Settings {
  val password = EnvConfig.getEmailUtilsPassword
}

object EmailUtils {
  // Email configuration
  val smtpHost = EnvConfig.getSMPTHost
  val smtpPort = EnvConfig.getSMPTPort
  val senderEmail = EnvConfig.getEmailSender
  val senderName = "Kafka Consumer"
  val senderPassword = Settings.password

  def sendEmail(toEmail: String, subject: String, body: String): Unit = {
    val properties = new Properties()
    properties.put("mail.smtp.host", smtpHost)
    properties.put("mail.smtp.port", smtpPort)
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")
    properties.put("mail.smtp.socketFactory.port", "465")
    properties.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory")

    // Create a Session with the email properties and authentication
    val session = Session.getInstance(properties, new Authenticator {
      override def getPasswordAuthentication: PasswordAuthentication = {
        new PasswordAuthentication(senderEmail, senderPassword)
      }
    })

    try {
      // Create a new MimeMessage object
      val message = new MimeMessage(session)

      // Set the recipient, sender, subject, and content
      message.setFrom(new InternetAddress(senderEmail, senderName)) // Include name in "From" field
      message.setRecipient(Message.RecipientType.TO, new InternetAddress(toEmail))
      message.setSubject(subject)
      message.setText(body)

      // Send the email
      Transport.send(message)
    } catch {
      case e: MessagingException =>
        println(s"Failed to send email: ${e.getMessage}")
    }
  }
}