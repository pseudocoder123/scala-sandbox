package repositories

import models.enums.{EventStatus, TaskStatus, TeamType}
import models.enums.TeamType.TeamType
import models.enums.EventStatus.EventStatus
import models.enums.TaskStatus.TaskStatus
import slick.jdbc.MySQLProfile.api._

private[repositories] object ColumnMappings {

  // Mapping for TeamType enum
  implicit val equipmentTypeColumnType: BaseColumnType[TeamType] =
    MappedColumnType.base[TeamType, String](
      e => e.toString,
      s => TeamType.withName(s)
    )

  // Mapping for EventType enum
  implicit val eventTypeColumnType: BaseColumnType[EventStatus] =
    MappedColumnType.base[EventStatus, String](
      e => e.toString,
      s => EventStatus.withName(s)
    )

  // Mapping for TaskStatus enum
  implicit val taskStatusColumnType: BaseColumnType[TaskStatus] =
    MappedColumnType.base[TaskStatus, String](
      e => e.toString,
      s => TaskStatus.withName(s)
    )
}