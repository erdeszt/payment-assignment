package challenge

import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.implicits._
import doobie.util.transactor.Transactor
import fs2._
import org.http4s.implicits._
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.Logger

import scala.concurrent.ExecutionContext.global

object Server {
  val resources = Resources.MySQL("root", "root")

  def stream(tx: Transactor[IO])(implicit F: ConcurrentEffect[IO], T: Timer[IO]): Stream[IO, Nothing] = {
    val invoicesImpl = Invoices.impl(tx)
    val paymentsImpl = Payments.impl(tx)
    val payersImpl = Payers.impl(tx)

    // Compose our various routes. They are tried in turn until one matches.
    // If none match, we return a 404 response
    val httpApp = (
      Invoices.routes(invoicesImpl) <+>
        Payments.routes(paymentsImpl) <+>
        Payers.routes(payersImpl)
    ).orNotFound

    // With Middlewares in place
    val finalHttpApp = Logger.httpApp(logHeaders = true, logBody = true)(httpApp)

    BlazeServerBuilder[IO](global)
      .bindHttp(8080, "0.0.0.0")
      .withHttpApp(finalHttpApp)
      .serve
  }.drain
}
