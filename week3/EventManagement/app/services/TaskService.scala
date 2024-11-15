package services

import models.AssignTasksRequest
import models.entity.Task
import repositories.TaskRepository

import java.time.{Duration, LocalDateTime}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class TaskService @Inject() (
                               taskRepository: TaskRepository,
                               kafkaProducerFactory: KafkaProducerFactory
                             )(implicit executionContext: ExecutionContext) {
  def create(task: Task): Future[Long] = taskRepository.create(task)

  def getEventById(taskId: Long): Future[Task] = taskRepository.getEventById(taskId)

  def updateStatus(taskId: Long, status: String): Future[Task] = {
    taskRepository.updateStatus(taskId, status)
  }

  def getTasksForEventId(eventId: Long): Future[Seq[Task]] = taskRepository.getTasksForEventId(eventId)

  def assignTasks(req: AssignTasksRequest): Future[List[Task]] = {
    val tasksList = req.tasks.foldLeft(List.empty[Task])((acc, ele) => {
      val task = Task(
        eventId =  req.eventId,
        teamId = ele.teamId,
        taskDescription = ele.taskDescription,
        deadLine = ele.deadLine,
        specialInstructions = ele.specialInstructions,
        status = "ASSIGNED",
        createdAt = LocalDateTime.now().toString
      )
      acc :+ task
    })
    taskRepository.assignTasks(tasksList).map { lists =>
      kafkaProducerFactory.sendTasksAssignmentList(lists)
      queueProgressRemainderNotifications(lists)
      lists
    }
  }

  def queueProgressRemainderNotifications(tasks: List[Task]): Unit = {
    val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

    tasks.foreach { task =>
      val deadline = LocalDateTime.parse(task.deadLine) // Assuming deadLine is in correct format
      val now = LocalDateTime.now()

      // Calculate the notification times: 3 hours, 1 day, and 6 hours prior to deadline
      val timesToNotify = List(
        (deadline.minusHours(3), 3),
        (deadline.minusDays(1), 1),
        (deadline.minusHours(6), 6)
      )

      timesToNotify.foreach { notifyTime =>
        // If the notify time is in the future, schedule the notification
        if (now.isBefore(notifyTime._1)) {
          val delay = Duration.between(now, notifyTime._1).toMillis
          scheduler.schedule(
            new Runnable {
              override def run(): Unit = {
                // Send Kafka notification for task reminder
                kafkaProducerFactory.queueNotifications(task, notifyTime._2)
              }
            },
            delay,
            TimeUnit.MILLISECONDS
          )
        }
      }
    }
  }
}
