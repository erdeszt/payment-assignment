package challenge

import java.nio.file.Paths

import cats.effect.{Blocker, ContextShift, IO, Resource}
import cats.implicits._
import doobie.h2.H2Transactor
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor
import doobie.util.update.Update0
import fs2._

/** The resources needed by [[Server]]
  *
  * Unless you want to implement support for a new DBMS, you shouldn't need to change anything here
  */
trait Resources[T <: Transactor[IO]] {
  def apply(implicit cs: ContextShift[IO]): Resource[IO, (T, Blocker)]
  def applyMigrations(tx: T, be: Blocker)(implicit cs: ContextShift[IO]): IO[Unit]
}

object Resources {
  val H2 = new Resources[H2Transactor[IO]] {
    override def apply(implicit cs: ContextShift[IO]): Resource[IO, (H2Transactor[IO], Blocker)] =
      for {
        ec <- ExecutionContexts.fixedThreadPool[IO](32) // our connect EC
        be <- Blocker[IO] // our blocking EC
        xa <- H2Transactor.newH2Transactor[IO](
          "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", // connect URL
          "sa", // username
          "", // password
          ec, // await connection here
          be // execute JDBC operations here
        )
      } yield (xa, be)

    override def applyMigrations(tx: H2Transactor[IO], be: Blocker)(implicit cs: ContextShift[IO]): IO[Unit] = {
      val pwd = Paths.get(".").toAbsolutePath

      for {
        migrations <- fs2.io.file
          .readAll[IO](Paths.get(pwd.toString, "db/migrations.sql"), be, 8192)
          .through(text.utf8Decode)
          .through(text.lines)
          .compile
          .string
        queries = migrations.split(';').toList.traverse(query => Update0(s"$query", None).run)
        _ <- queries.transact(tx).void
      } yield ()
    }
  }

  def MySQL(user: String, password: String) = new Resources[HikariTransactor[IO]] {
    override def apply(implicit cs: ContextShift[IO]): Resource[IO, (HikariTransactor[IO], Blocker)] = for {
      ec <- ExecutionContexts.fixedThreadPool[IO](32)
      blockingThreadPool <- ExecutionContexts.cachedThreadPool[IO]
      blocker = Blocker.liftExecutionContext(blockingThreadPool)
      xa <-
        HikariTransactor
          .newHikariTransactor[IO](
            "com.mysql.cj.jdbc.Driver",
            "jdbc:mysql://127.0.0.1:3306/challenge",
            user,
            password,
            ec,
            blocker
          )
    } yield (xa, blocker)

    override def applyMigrations(tx: HikariTransactor[IO], be: Blocker)(implicit cs: ContextShift[IO]): IO[Unit] =
      IO.unit
  }
}
