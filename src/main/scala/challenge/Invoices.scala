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

trait Invoices[F[_]] {
  def get(id: Invoices.Id): F[Option[Invoices.Invoice]]
  def create(invoice: Invoices.New): F[Invoices.Id]
}

object Invoices {

  /** An [[Invoice]] is a document issued to a given [[Payers.Payer]],
    * detailing an amount owed by that payer for various goods and services provided.
    *
    * Here we are omitting the details, and only care about the amount owed and
    * who needs to pay said amount.
    *
    * To make things easy, an invoice is payable immediately after being sent,
    * so the amount on the invoice is owed immediately after the invoice has been sent.
    *
    * @param total The total payable amount on the invoice. Usually negative.
    * @param payerId The ID of the payer who receives the invoice
    * @param sentAt The time at which the invoice becomes payable
    */
  final case class Invoice(invoiceId: Int, total: Double, payerId: Int, sentAt: LocalDateTime)
  object Invoice {
    // JSON codec for marshalling to-and-from JSON
    implicit val codec: Codec[Invoice] = deriveCodec[Invoice]

    // Codecs for reading/writing HTTP entities (uses the above JSON codec)
    implicit def entityDecoder[F[_]: Sync]: EntityDecoder[F, Invoice] = jsonOf
    implicit def entityEncoder[F[_]: Applicative]: EntityEncoder[F, Invoice] = jsonEncoderOf
  }

  final case class New(total: Double, payerId: Int, sentAt: Option[LocalDateTime])
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

  def impl(tx: Transactor[IO]): Invoices[IO] = new Invoices[IO] {
    override def get(invoiceId: Id): IO[Option[Invoice]] = {
      sql"""SELECT invoiceId, total, payerId, sentAt FROM invoice WHERE invoiceId = ${invoiceId.id}"""
        .query[Invoice]
        .to[List]
        .transact(tx)
        .map(_.headOption)
    }

    override def create(newInvoice: New): IO[Id] = {
      val sentAt = newInvoice.sentAt
        .getOrElse(LocalDateTime.now())
        .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

      val q = for {
        id <- sql"""INSERT INTO invoice (total, payerId, sentAt)
                  |VALUES (${newInvoice.total}, ${newInvoice.payerId}, $sentAt)
                 """.stripMargin.update.withUniqueGeneratedKeys[Int]("invoiceId")
      } yield Id(id)

      q.transact(tx)
    }
  }

  def routes(invoices: Invoices[IO]): HttpRoutes[IO] = {
    val dsl = new Http4sDsl[IO] {}
    import dsl._

    HttpRoutes.of[IO] {
      case GET -> Root / "invoice" / IntVar(invoiceId) =>
        invoices.get(Id(invoiceId)).flatMap {
          case Some(invoice) => Ok(invoice)
          case None          => NotFound()
        }

      case req @ POST -> Root / "invoice" =>
        req.decode[New] { input =>
          invoices.create(input).flatMap(Ok(_))
        }
    }
  }
}
