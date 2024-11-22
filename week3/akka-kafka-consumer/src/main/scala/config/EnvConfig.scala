package config

object EnvConfig {
  def getKafkaBroker: String = sys.env.getOrElse("KAFKA_BROKERS", "localhost:9092")
  def getEmailRecipient: String = sys.env.getOrElse("EMAIL_RECIPIENT", "")
  def getEmailSender: String = sys.env.getOrElse("EMAIL_SENDER", "")
  def getEmailUtilsPassword: String = sys.env.getOrElse("EMAIL_UTILS_PASSWORD", "")
  def getSMPTHost: String = sys.env.getOrElse("SMPT_HOST", "smtp.gmail.com") // SMTP server
  def getSMPTPort: String = sys.env.getOrElse("SMPT_PORT", "587") // SMTP port (use 465 for SSL, 587 for TLS)
}