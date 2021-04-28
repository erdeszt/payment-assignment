package challenge

import java.time.{LocalDate, ZoneOffset}

import cats.Applicative
import cats.effect.{IO, Sync}
import doobie.implicits._
import doobie.implicits.javatime._
import doobie.util.transactor.Transactor
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.dsl.Http4sDsl
import org.http4s.{EntityDecoder, EntityEncoder, HttpRoutes}

trait Payers[F[_]] {
  def get(id: Payers.Id): F[Option[Payers.Payer]]
  def create(payer: Payers.New): F[Payers.Id]
  def balance(payerId: Payers.Id, date: LocalDate): F[Payers.Balance]
}

object Payers {

  /** A [[Payer]] is a person or entity to which invoices can be sent
    * and from which payments can be received.
    */
  final case class Payer(payerId: Int, name: String)
  object Payer {
    // JSON codec for marshalling to-and-from JSON
    implicit val codec: Codec[Payer] = deriveCodec[Payer]

    // Codecs for reading/writing HTTP entities (uses the above JSON codec)
    implicit def entityDecoder[F[_]: Sync]: EntityDecoder[F, Payer] = jsonOf
    implicit def entityEncoder[F[_]: Applicative]: EntityEncoder[F, Payer] = jsonEncoderOf
  }

  final case class New(name: String)
  object New {
    implicit val codec: Codec[New] = deriveCodec[New]
    implicit def entityDecoder[F[_]: Sync]: EntityDecoder[F, New] = jsonOf
    implicit def entityEncoder[F[_]: Applicative]: EntityEncoder[F, New] = jsonEncoderOf
  }
  final case class Id(id: Int)
  object Id {
    implicit val codec: Codec[Id] = deriveCodec[Id]
    implicit def entityDecoder[F[_]: Sync]: EntityDecoder[F, Id] = jsonOf
    implicit def entityEncoder[F[_]: Applicative]: EntityEncoder[F, Id] = jsonEncoderOf
  }

  final case class Balance(payerId: Int, balance: Double)
  object Balance {
    implicit val codec: Codec[Balance] = deriveCodec[Balance]
    implicit def entityDecoder[F[_]: Sync]: EntityDecoder[F, Balance] = jsonOf
    implicit def entityEncoder[F[_]: Applicative]: EntityEncoder[F, Balance] = jsonEncoderOf
  }

  def impl(tx: Transactor[IO]): Payers[IO] = new Payers[IO] {
    override def get(payerId: Id): IO[Option[Payer]] = {
      sql"""SELECT payerId, name FROM payer WHERE payerId = ${payerId.id}"""
        .query[Payer]
        .to[List]
        .transact(tx)
        .map(_.headOption)
    }

    override def create(newPayer: New): IO[Id] = {
      val q = for {
        id <- {
          sql"""INSERT INTO payer (name)
               |VALUES (${newPayer.name})
             """.stripMargin.update.withUniqueGeneratedKeys[Int]("payerId")
        }
      } yield Id(id)

      q.transact(tx)
    }

    override def balance(payerId: Id, date: LocalDate): IO[Balance] = {
      val invoicesIO = {
        sql"""SELECT invoiceId, total, payerId, sentAt FROM invoice
             |WHERE payerId = ${payerId.id}
           """.stripMargin
          .query[Invoices.Invoice]
          .to[List]
          .transact(tx)
      }

      val paymentsIO = {
        sql"""SELECT paymentId, amount, payerId, receivedAt FROM payment
             |WHERE payerId = ${payerId.id}
           """.stripMargin
          .query[Payments.Payment]
          .to[List]
          .transact(tx)
      }

      for {
        invoices <- invoicesIO
        payments <- paymentsIO
      } yield Balance(payerId.id, calculateBalance(date, invoices, payments))
    }

    /** The balance for a given [[Payer]] is the amount that the payer still has left to pay (if negative)
      * or has paid in advance (if positive) on a given date (at midnight).
      *
      * If the balance is zero, then all invoices sent before the given date have been paid in full,
      * and nothing has been paid in advance.
      *
      * @param date The date for which the balance should be calculated
      * @param allInvoices The invoices to take into account for the balance
      * @param allPayments The payments to take into account for the balance
      */
    private def calculateBalance(
      date: LocalDate,
      allInvoices: List[Invoices.Invoice],
      allPayments: List[Payments.Payment]
    ): Double = {
      val invoices =
        allInvoices
          .map(invoice => (invoice, invoice.sentAt.toEpochSecond(ZoneOffset.UTC)))
          .sortBy(_._2)
          .takeWhile(date.toEpochDay <= _._2)
          .map { case (invoice, _) => invoice }

      val payments =
        allPayments
          .map(payment => (payment, payment.receivedAt.toEpochSecond(ZoneOffset.UTC)))
          .sortBy(_._2)
          .takeWhile(date.toEpochDay <= _._2)
          .map { case (payment, _) => payment }

      invoices.map(_.total).sum + payments.map(_.amount).sum
    }
  }

  def routes(payers: Payers[IO]): HttpRoutes[IO] = {
    val dsl = new Http4sDsl[IO] {}
    import dsl._

    HttpRoutes.of[IO] {
      case GET -> Root / "payer" / IntVar(payerId) =>
        payers.get(Id(payerId)).flatMap {
          case Some(payer) => Ok(payer)
          case None        => NotFound()
        }

      case req @ POST -> Root / "payer" =>
        req.decode[New] { input =>
          payers.create(input).flatMap(Ok(_))
        }

      case GET -> Root / "payer" / payerId / "balance" / date =>
        payers.balance(Id(payerId.toInt), LocalDate.parse(date)).flatMap(Ok(_))
    }
  }
}
