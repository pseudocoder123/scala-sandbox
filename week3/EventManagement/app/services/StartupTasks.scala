package services

import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}
import repositories.{EventRepository, TaskRepository}
import java.time.{Duration, LocalDate, LocalDateTime, LocalTime}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

class StartupTasks @Inject()(eventRepository: EventRepository,
                             kafkaProducerFactory: KafkaProducerFactory,
                             taskRepository: TaskRepository)(implicit ec: ExecutionContext) {

  // Initial time after the application startup when this runs
  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  startDailyOverdueCheck()
//  startPreciseNotificationScheduler()

  private def startDailyOverdueCheck(): Unit = {
    // Schedule the task to run daily at the specified time
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          checkEventDayAlert()
          checkPreparationRemainder()
        }
      },
      0L,
      TimeUnit.DAYS.toSeconds(1), // Repeat every 24 hours
      TimeUnit.SECONDS
    )
  }

  // Method to check for Event Day Alert
  private def checkEventDayAlert(): Unit = {
    val currentDate = LocalDate.now()

    eventRepository.getEventsByDate(currentDate: LocalDate).flatMap { events =>
      Future.sequence(
        events.map { event =>
          taskRepository.getTasksForEventId(event.id.get).map { tasks =>
            kafkaProducerFactory.sendEventAlerts(event, tasks, isEventDay = true)
          }
        }
      )
    }.recover {
      case ex: Exception =>
        println(s"Failed to check overdue allocations: ${ex.getMessage}")
    }
  }

  // Preparation Remainder one day before
  private def checkPreparationRemainder(): Unit = {
    val eventDate = LocalDate.now().plusDays(1)

    // Retrieve overdue allocations as a Future[Seq[EquipmentAllocation]]
    eventRepository.getEventsByDate(eventDate: LocalDate).flatMap { events =>
      Future.sequence(
        events.map { event =>
          taskRepository.getTasksForEventId(event.id.get).map { tasks =>
            kafkaProducerFactory.sendEventAlerts(event, tasks, isEventDay = false)
          }
        }
      )
    }.recover {
      case ex: Exception =>
        println(s"Failed to check overdue allocations: ${ex.getMessage}")
    }
  }

//  private def startPreciseNotificationScheduler(): Unit = {
//    // Run this check every 6 hours to schedule precise notifications
//    scheduler.scheduleAtFixedRate(
//      new Runnable {
//        override def run(): Unit = {
//          scheduleUpcomingEventNotifications()
//        }
//      },
//      0L,
//      TimeUnit.HOURS.toSeconds(3),
//      TimeUnit.SECONDS
//    )
//  }
//
//  private def scheduleUpcomingEventNotifications(): Unit = {
//    val now = LocalDate.now()
//
//    eventRepository.getEventsByDate(now).flatMap { events =>
//      Future.sequence(
//        events.map { event =>
//          scheduleEventNotifications(event)
//        }
//      )
//    }.recover {
//      case ex: Exception =>
//        println(s"Failed to schedule upcoming notifications: ${ex.getMessage}")
//    }
//  }
//
//  private def scheduleEventNotifications(event: Event): Future[Unit] = {
//    // Calculate notification times (e.g., 3 hours before, 1 hour before)
//    taskRepository.getTasksForEventId(event.id.get).map {tasks =>
//      tasks.foreach { task =>
//        scheduleTaskNotifications(event, task)
//      }
//    }
//  }
//
//  private def scheduleTaskNotifications(event: Event, task: Task): Unit = {
//    def convertStringToLocalDateTime(dateString: String): LocalDateTime = {
//      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//      LocalDateTime.parse(dateString, formatter)
//    }
//
//    val now = LocalDateTime.now()
//
//    val notificationTimes = Seq(
//      (now.plusHours(3), 3),
//      (now.plusHours(1), 1)
//    ).filter { case (time, _) => time.getHour == now.getHour }
//
//    notificationTimes.foreach { case (notificationTime, timeDescription) =>
//      scheduleOneTimeNotification(
//        scheduledTime = notificationTime,
//        event = event,
//        task = task,
//        timeDescription = timeDescription
//      )
//    }
//  }
//
//  private def scheduleOneTimeNotification(
//                                           scheduledTime: LocalDateTime,
//                                           event: Event,
//                                           task: Task,
//                                           timeDescription: Int
//                                         ): Unit = {
//    val now = LocalDateTime.now()
//    val delay = Duration.between(now, scheduledTime)
//
//    if(delay.isNegative) return
//
//    scheduler.schedule(
//      new Runnable {
//        override def run(): Unit = {
//          kafkaProducerFactory.sendPeriodicUpdate(event, task, timeDescription)
//        }
//      },
//      5,
//      TimeUnit.SECONDS
//    )
//  }

}