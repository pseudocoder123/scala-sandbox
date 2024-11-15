package services

import models.entity.Team
import repositories.TeamRepository

import javax.inject.{Inject, Singleton}
import scala.concurrent.Future

@Singleton
class TeamService @Inject() (
                                  teamRepository: TeamRepository
                                ) {
  def create(team: Team): Future[Long] = teamRepository.create(team)

  def getTeamDetailsById(teamId: Long): Future[Team] = teamRepository.getTeamDetailsById(teamId)

  def listTeams(teamType: Option[String]): Future[Seq[Team]] = teamRepository.listTeams(teamType)
}
