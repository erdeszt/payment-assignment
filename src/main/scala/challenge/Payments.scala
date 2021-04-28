package challenge

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

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

trait Payments[F[_]] {
  def get(id: Payments.Id): F[Option[Payments.Payment]]
  def create(payment: Payments.New): F[Payments.Id]
}

object Payments {

  /** A [[Payment]] represents an amount of money paid against an [[Invoices.Invoice]] issued to the given [[Payers.Payer]].
    *
    * Payments can be made in advance, so it is fine for the amount on a payment to be greater than the total amount owed.
    *
    * @param amount The amount paid by the [[Payers.Payer]]. Usually positive.
    * @param payerId The ID of the [[Payers.Payer]] paying the given amount.
    * @param receivedAt The time at which the payment was made
    */
  final case class Payment(paymentId: Int, amount: Double, payerId: Int, receivedAt: LocalDateTime)
  object Payment {
    // JSON codec for marshalling to-and-from JSON
    implicit val encoder: Codec[Payment] = deriveCodec[Payment]

    // Codecs for reading/writing HTTP entities (uses the above JSON codec)
    implicit def entityDecoder[F[_]: Sync]: EntityDecoder[F, Payment] = jsonOf
    implicit def entityEncoder[F[_]: Applicative]: EntityEncoder[F, Payment] = jsonEncoderOf
  }

  final case class New(amount: Double, payerId: Int, receivedAt: Option[LocalDateTime])
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

  def impl(tx: Transactor[IO]): Payments[IO] = new Payments[IO] {
    override def get(paymentId: Id): IO[Option[Payment]] = {
      sql"""SELECT paymentId, amount, payerId, receivedAt FROM payment WHERE paymentId = ${paymentId.id}"""
        .query[Payment]
        .to[List]
        .transact(tx)
        .map(_.headOption)
    }

    override def create(newPayment: New): IO[Id] = {
      val receivedAt = newPayment.receivedAt
        .getOrElse(LocalDateTime.now())
        .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

      val q = for {
        id <- sql"""INSERT INTO payment (amount, payerId, receivedAt)
                  |VALUES (${newPayment.amount}, ${newPayment.payerId}, $receivedAt)
                 """.stripMargin.update.withUniqueGeneratedKeys[Int]("paymentId")
      } yield Id(id)

      q.transact(tx)
    }
  }

  def routes(payments: Payments[IO]): HttpRoutes[IO] = {
    val dsl = new Http4sDsl[IO] {}
    import dsl._

    HttpRoutes.of[IO] {
      case GET -> Root / "payment" / IntVar(paymentId) =>
        payments.get(Id(paymentId)).flatMap {
          case Some(payment) => Ok(payment)
          case None          => NotFound()
        }

      case req @ POST -> Root / "payment" =>
        req.decode[New] { input =>
          payments.create(input).flatMap(Ok(_))
        }
    }
  }
}
