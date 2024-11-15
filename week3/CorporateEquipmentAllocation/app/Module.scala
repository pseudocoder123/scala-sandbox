import play.api.inject.{SimpleModule, _}
import services.StartupTasks

class Module extends SimpleModule(bind[StartupTasks].toSelf.eagerly())