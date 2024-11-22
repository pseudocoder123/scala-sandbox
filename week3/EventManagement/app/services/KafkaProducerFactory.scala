package services

import config.EnvConfig
import models.entity.{Event, Issue, Task}
import models.request.KafkaMessageFormat
import play.api.libs.json._

import javax.inject._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object MessageTeam {
  val CATERING = "CATERING"
  val ENTERTAINMENT = "ENTERTAINMENT"
  val DECORATIONS = "DECORATIONS"
  val LOGISTICS = "LOGISTICS"
  val MANAGER = "MANAGER"
}

@Singleton
class KafkaProducerFactory @Inject()() {
  private val props = new Properties() { props =>
    props.put("bootstrap.servers", EnvConfig.getKafkaBroker)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  }

  private val producer = new KafkaProducer[String, String](props)
  private val TOPIC = "event-management-topic"

  private def determineReceiver(teamId: Long): String = teamId match {
    case 1 => MessageTeam.CATERING
    case 2 => MessageTeam.ENTERTAINMENT
    case 3 => MessageTeam.DECORATIONS
    case 4 => MessageTeam.LOGISTICS
  }

  private def createKafkaMessage(receiver: String, messageType: String, message: String): ProducerRecord[String, String] = {
    val kafkaMessage = KafkaMessageFormat(
      receiver = receiver,
      messageType = messageType,
      message = message
    )
    new ProducerRecord[String, String](TOPIC, Json.stringify(Json.toJson(kafkaMessage)))
  }

  def sendTasksAssignmentList(tasks: List[Task]): Unit = {
    tasks.foreach { task =>
      val message = s"Task is allocated for eventId: #${task.eventId} with description: ${task.taskDescription}, deadline: ${task.deadLine}" +
        task.specialInstructions.map(instructions => s" and specialInstructions: $instructions").getOrElse("")

      val record = createKafkaMessage(
        receiver = determineReceiver(task.teamId),
        messageType = "TASK_ALLOCATION",
        message = message
      )

      producer.send(record)
    }
  }

  def sendIssueReport(issue: Issue): Unit = {
    val message = s"${issue.issueType} in #${issue.taskId} taskId for eventId: #${issue.eventId} from teamId #${issue.teamId}, " +
      s"reportedAt: ${issue.reportedAt} with description: ${issue.issueDescription}"

    val record = createKafkaMessage(
      receiver = MessageTeam.MANAGER,
      messageType = "ISSUE_ALERT",
      message = message
    )

    producer.send(record)
  }

  def sendEventAlerts(event: Event, tasks: Seq[Task], isEventDay: Boolean): Unit = {
    // Send team-specific alerts
    tasks.foreach { task =>
      val message = if (isEventDay) s"Event Day Alert - Please complete taskId: #${task.id.get}"
      else s"Preparation Remainder - Please complete taskId: ${task.id.get}"

      val record = createKafkaMessage(
        receiver = determineReceiver(task.teamId),
        messageType = if (isEventDay) "EVENT_DAY_ALERT" else "PREPARATION_REMAINDER",
        message = message
      )

      producer.send(record)
    }

    // Send manager alert
    val managerMessage = if (isEventDay) s"Event Day Alert for eventId - #${event.id.get}"
    else s"Preparation Remainder - Please complete eventId: #${event.id.get}"

    val managerRecord = createKafkaMessage(
      receiver = MessageTeam.MANAGER,
      messageType = if (isEventDay) "EVENT_DAY_ALERT" else "PREPARATION_REMAINDER",
      message = managerMessage
    )

    producer.send(managerRecord)
  }

  def sendPeriodicUpdate(event: Event, task: Task, timeDescription: Int): Unit = {
    val record = createKafkaMessage(
      receiver = determineReceiver(task.teamId),
      messageType = "PREPARATION_UPDATE",
      message = s"Complete your task: ${task.id} for event: #${event.id}, remaining: $timeDescription hours"
    )

    producer.send(record)
  }

  def queueNotifications(task: Task, time: Int): Unit = {
    val record = createKafkaMessage(
      receiver = determineReceiver(task.teamId),
      messageType = "PREPARATION_UPDATE",
      message = s"Complete your task: #${task.id}, remaining: $time hours"
    )

    producer.send(record)
  }
}