package challenge

import java.time.{LocalDate, ZoneOffset}
import cats.{Applicative, Functor}
import cats.syntax.applicative._
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
        sql"""SELECT sum(total) FROM invoice
             |WHERE payerId = ${payerId.id} and sentAt < ${date}
           """.stripMargin
          .query[Double]
          .unique
          .transact(tx)
      }

      val paymentsIO = {
        sql"""SELECT sum(amount) FROM payment
             |WHERE payerId = ${payerId.id} and receivedAt < ${date}
           """.stripMargin
          .query[Double]
          .unique
          .transact(tx)
      }

      for {
        invoiceSum <- invoicesIO
        paymentSum <- paymentsIO
      } yield Balance(payerId.id, invoiceSum + paymentSum)
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
