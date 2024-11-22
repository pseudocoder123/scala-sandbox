package services

import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}
import repositories.{AllocationRepository, EmployeeRepository, EquipmentRepository}

import java.time.{Duration, LocalDate, LocalDateTime, LocalTime}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

class StartupTasks @Inject()(allocationRepository: AllocationRepository,
                             kafkaProducerFactory: KafkaProducerFactory,
                             employeeRepository: EmployeeRepository)(implicit ec: ExecutionContext) {

  // Initial time after the application startup when this runs
  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  startDailyOverdueCheck()

  def startDailyOverdueCheck(): Unit = {
    // Schedule the task to run daily at the specified time
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          checkForOverdueAllocations()
        }
      },
      0L,
      TimeUnit.DAYS.toSeconds(1), // Repeat every 24 hours (TimeUnit.DAYS.toSeconds(1))
      TimeUnit.SECONDS
    )
  }

  // Method to check overdue equipment allocations and send reminders
  private def checkForOverdueAllocations(): Unit = {
    val currentDate = LocalDate.now()

    // Retrieve overdue allocations as a Future[Seq[EquipmentAllocation]]
    allocationRepository.getOverdueAllocationDetails(currentDate).flatMap { overdueAllocations =>
      println(s"Length of overdue allocations: ${overdueAllocations.length}")
      Future.sequence(
        overdueAllocations.map { allocation =>
          employeeRepository.getEmployeeById(allocation.employeeId).map {
            case Some(employee) => kafkaProducerFactory.sendMessageOverdueNotification(allocation, employee)
            case None => ()
          }
        }
      )
    }.recover {
      case ex: Exception =>
        println(s"Failed to check overdue allocations: ${ex.getMessage}")
    }
  }

}