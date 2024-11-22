package config

object EnvConfig {
  def getKafkaBroker: String = sys.env.getOrElse("KAFKA_BROKERS", "localhost:9092")
  def getJwtUtilSecretKey: String = sys.env.getOrElse("JWT_UTIL_SECRET_KEY", "corporate-equip-jwt-key")
  def getJwtUtilIssuer: String = sys.env.getOrElse("JWT_UTIL_ISSUER", "corporate-equip-play-service")
}