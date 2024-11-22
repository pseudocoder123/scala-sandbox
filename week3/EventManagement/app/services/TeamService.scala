package services

import models.entity.Team
import models.enums.TeamType.TeamType
import repositories.TeamRepository
import javax.inject.{Inject, Singleton}
import scala.concurrent.Future

@Singleton
class TeamService @Inject() (
                                  teamRepository: TeamRepository
                                ) {
  def create(team: Team): Future[Long] = teamRepository.create(team)

  def getTeamDetailsById(teamId: Long): Future[Team] = teamRepository.getTeamDetailsById(teamId)

  def listTeams(teamType: Option[TeamType]): Future[Seq[Team]] = teamRepository.listTeams(teamType)
}
