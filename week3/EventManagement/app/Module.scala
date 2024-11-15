import play.api.inject._
import services.StartupTasks

class Module extends SimpleModule(bind[StartupTasks].toSelf.eagerly())