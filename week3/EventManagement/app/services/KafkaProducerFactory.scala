package services

import models.entity.{Event, Issue, Task}
import models.KafkaMessageFormat
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
  private val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[String, String](props)

  def sendTasksAssignmentList(lists: List[Task]): Unit = {
    val producerRecords: List[ProducerRecord[String, String]] = lists.map { task =>
      var message = s"Task is allocated for eventId: ${task.eventId} with description: ${task.taskDescription}, deadline: ${task.deadLine}"
      if(task.specialInstructions.isDefined) message += s" and specialInstructions: ${task.specialInstructions.get}"

      val receiver = task.teamId match {
        case 1 => MessageTeam.CATERING
        case 2 => MessageTeam.ENTERTAINMENT
        case 3 => MessageTeam.DECORATIONS
        case 4 => MessageTeam.LOGISTICS
      }

      val kafkaMessageFormat = KafkaMessageFormat(
        receiver=receiver,
        messageType="TASK_ALLOCATION",
        message= message
      )

      val jsonMessage: String = Json.stringify(Json.toJson(kafkaMessageFormat))
      new ProducerRecord[String, String]("event-management-topic", jsonMessage)
    }

    producerRecords.foreach(record => producer.send(record))
  }

  def sendIssueReport(issue: Issue): Unit = {
    val record: ProducerRecord[String, String] = {
      var message =
        s"""${issue.issueType} in ${issue.taskId} taskId for event(${issue.eventId}) from team(${issue.teamId}), reportedAt: ${issue.reportedAt} with description: ${issue.issueDescription}""".stripMargin

      val kafkaMessageFormat = KafkaMessageFormat(
        receiver=MessageTeam.MANAGER,
        messageType="ISSUE_ALERT",
        message= message
      )

      val jsonMessage: String = Json.stringify(Json.toJson(kafkaMessageFormat))
      new ProducerRecord[String, String]("event-management-topic", jsonMessage)
    }

    producer.send(record)
  }

  def sendEventAlerts(event: Event, tasks: Seq[Task], isEventDay: Boolean): Unit = {
    val producerRecords: Seq[ProducerRecord[String, String]] = tasks.map { task =>
      var message = if(isEventDay) s"Event Day Alert - Please complete taskId: ${task.id.get}"
      else s"Preparation Remainder - Please complete taskId: ${task.id.get}"

      val receiver = task.teamId match {
        case 1 => MessageTeam.CATERING
        case 2 => MessageTeam.ENTERTAINMENT
        case 3 => MessageTeam.DECORATIONS
        case 4 => MessageTeam.LOGISTICS
      }

      val kafkaMessageFormat = KafkaMessageFormat(
        receiver=receiver,
        messageType=if(isEventDay) "EVENT_DAY_ALERT" else "PREPARATION_REMAINDER",
        message= message
      )

      val jsonMessage: String = Json.stringify(Json.toJson(kafkaMessageFormat))
      new ProducerRecord[String, String]("event-management-topic", jsonMessage)
    }

    producerRecords.foreach(record => producer.send(record))

    // Send Alert For Manager
    val managerEventAlertMessage = Json.stringify(Json.toJson(KafkaMessageFormat(
              receiver=MessageTeam.MANAGER,
              messageType=if(isEventDay) "EVENT_DAY_ALERT" else "PREPARATION_REMAINDER",
              message=if(isEventDay) s"Event Day Alert for event - ${event.id.get}"
              else s"Preparation Remainder - Please complete taskId: ${event.id.get}"
    )))
    producer.send(new ProducerRecord[String, String]("event-management-topic", managerEventAlertMessage))

  }

//  def sendPeriodicUpdate(event: Event, task: Task, timeDescription: Int): Unit = {
//    val receiver = task.teamId match {
//      case 1 => MessageTeam.CATERING
//      case 2 => MessageTeam.ENTERTAINMENT
//      case 3 => MessageTeam.DECORATIONS
//      case 4 => MessageTeam.LOGISTICS
//    }
//
//    val message = s"Complete your task: ${task.id} for event: ${event.id}, remaining: $timeDescription hours"
//
//    val kafkaMessageFormat = KafkaMessageFormat(
//      receiver=receiver,
//      messageType="PREPARATION_UPDATE",
//      message= message
//    )
//
//    val jsonMessage: String = Json.stringify(Json.toJson(kafkaMessageFormat))
//    producer.send(new ProducerRecord[String, String]("event-management-topic", jsonMessage))
//  }

  def queueNotifications(task: Task, time: Int): Unit = {
    val receiver = task.teamId match {
      case 1 => MessageTeam.CATERING
      case 2 => MessageTeam.ENTERTAINMENT
      case 3 => MessageTeam.DECORATIONS
      case 4 => MessageTeam.LOGISTICS
    }

    val message = s"Complete your task: ${task.id}, remaining: $time hours"

    val kafkaMessageFormat = KafkaMessageFormat(
      receiver=receiver,
      messageType="PREPARATION_UPDATE",
      message= message
    )

    val jsonMessage: String = Json.stringify(Json.toJson(kafkaMessageFormat))
    producer.send(new ProducerRecord[String, String]("event-management-topic", jsonMessage))
  }

  def sendDemo(): Unit = {
    val record: ProducerRecord[String, String] = {
      val kafkaMessageFormat = KafkaMessageFormat(
        receiver=MessageTeam.MANAGER,
        messageType="SEND_DEMO",
        message= "message"
      )

      val jsonMessage: String = Json.stringify(Json.toJson(kafkaMessageFormat))
      new ProducerRecord[String, String]("event-management-topic", jsonMessage)
    }

    producer.send(record)
  }

}