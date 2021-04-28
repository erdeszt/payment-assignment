package challenge

import cats.effect.{ExitCode, IO, IOApp}

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    Server.resources.apply.use { case (tx, blocker) =>
      Server.resources.applyMigrations(tx, blocker) *>
        Server.stream(tx).compile.drain.as(ExitCode.Success)
    }
  }
}
