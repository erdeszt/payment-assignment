package challenge

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import cats.Applicative
import cats.effect.{Async, IO, Sync}
import cats.instances.list._
import cats.instances.option._
import cats.Traverse.ops._
import doobie.ConnectionIO
import doobie.implicits._
import doobie.implicits.javatime._
import doobie.util.transactor.Transactor
import doobie.util.update.Update
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.dsl.Http4sDsl
import org.http4s.{EntityDecoder, EntityEncoder, HttpRoutes}

trait Payments[F[_]] {
  def get(id: Payments.Id): F[Option[Payments.Payment]]
  def create(payment: Payments.New): F[Payments.Id]
  def covers(id: Payments.Id): F[Option[Payments.Covers]]
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

  final case class Covers(invoices: List[Invoices.Id])
  object Covers {
    implicit val codec: Codec[Covers] = deriveCodec[Covers]
    implicit def entityDecoder[F[_]: Sync]: EntityDecoder[F, Covers] = jsonOf
    implicit def entityEncoder[F[_]: Applicative]: EntityEncoder[F, Covers] = jsonEncoderOf
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

    override def covers(id: Id): IO[Option[Covers]] = {
      // Query the payment to check if it exists and get the payer id, amount and payment date
      sql"SELECT payerId, amount, receivedAt FROM payment WHERE paymentId = ${id} LIMIT 1"
        .query[(Payers.Id, Double, LocalDateTime)]
        .option
        .flatMap {
          _.traverse[ConnectionIO, Covers] { case (payerId, amount, receivedAt) =>
            for {
              invoiced <- sql"""SELECT coalesce(sum(total), 0) FROM invoice
                              |WHERE payerId = ${payerId} and sentAt <= ${receivedAt}
                             """.stripMargin.query[Double].unique
              payed <- sql"""SELECT coalesce(sum(amount), 0) FROM payment
                            |WHERE payerId = ${payerId} and paymentId < ${id}
                           """.stripMargin.query[Double].unique
              balance = invoiced + payed

              // If balance is positive, all previous invoices are fully paid
              // Otherwise collect the previous unpaid invoices covered by the payment
              (previousCoveredInvoices, remainingMoney) <-
                if (balance >= 0) {
                  Applicative[ConnectionIO].pure((List.empty[Invoices.Id], amount))
                } else {
                  for {
                    // Invoices not fully covered by previous payments
                    unpaid <-
                      sql"""SELECT
                          |  i.invoiceId,
                          |  i.total,
                          |  (SELECT COALESCE(SUM(ABS(total))) FROM invoice WHERE payerId = ${payerId} AND invoiceId <= i.invoiceId) AS running_invoiced
                          |FROM invoice i
                          |WHERE i.payerId = ${payerId} AND i.sentAt < ${receivedAt}
                          |HAVING running_invoiced > ${payed}
                          |ORDER BY sentAt ASC
                         """.stripMargin.query[(Invoices.Id, Double, Option[Double])].to[List]
                  } yield unpaid.foldLeft((List.empty[Invoices.Id], amount)) {
                    case ((unpaids, remaining), (invoiceId, total, _)) =>
                      // Check if we can pay the current invoice
                      if (remaining > Math.abs(total)) { // TODO: Stop once reached an unpayable
                        (invoiceId :: unpaids, remaining + total)
                      } else {
                        (unpaids, remaining)
                      }
                  }
                }
              lastPaidId <- previousCoveredInvoices match {
                case Nil =>
                  sql"SELECT i.invoiceId, (SELECT COALESCE(SUM(ABS(total))) FROM invoice WHERE payerId = ${payerId} AND invoiceId < i.invoiceId) AS rt FROM invoice AS i WHERE payerId = ${payerId} HAVING rt < ${payed + amount} ORDER BY invoiceId DESC LIMIT 1"
                    .query[(Invoices.Id, Double)]
                    .option
                    .map(_.map(_._1).getOrElse(Invoices.Id(0)))
                case _ => Applicative[ConnectionIO].pure(previousCoveredInvoices.maxBy(_.id))
              }
              // Get the invoices after this payment that are within are remaining budget
              payableInvoices <-
                sql"""SELECT
                    | i.invoiceId,
                    | i.total,
                    | (SELECT COALESCE(SUM(ABS(total))) FROM invoice WHERE payerId = ${payerId} AND invoiceId < i.invoiceId AND invoiceId > ${lastPaidId}) AS running_invoiced
                    | FROM invoice i
                    | WHERE i.payerId = ${payerId} AND i.invoiceId > ${lastPaidId}
                    | HAVING COALESCE(running_invoiced, 0) <= ${remainingMoney}
                    | ORDER BY sentAt ASC
                   """.stripMargin.query[(Invoices.Id, Double, Option[Double])].to[List]
              totalPayable = payableInvoices.map(_._2).sum
              remainingAfterPayable = remainingMoney + totalPayable
              extraPayable <- payableInvoices match {
                case Nil                             => Applicative[ConnectionIO].pure(Option.empty[Invoices.Id])
                case _ if remainingAfterPayable <= 0 => Applicative[ConnectionIO].pure(Option.empty[Invoices.Id])
                case _ =>
                  val lastPaidId = payableInvoices.maxBy(_._1.id)._1
                  sql"SELECT invoiceId FROM invoice WHERE invoiceId > ${lastPaidId} AND payerId = ${payerId} ORDER BY sentAt ASC LIMIT 1"
                    .query[Invoices.Id]
                    .option
              }
            } yield Covers(previousCoveredInvoices ++ payableInvoices.map(_._1) ++ extraPayable.toList)

          }
        }
        .transact(tx)
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

      case GET -> Root / "payment" / IntVar(paymentId) / "covers" =>
        payments.covers(Id(paymentId)).flatMap {
          case Some(covers) => Ok(covers)
          case None         => NotFound()
        }

      case req @ POST -> Root / "payment" =>
        req.decode[New] { input =>
          payments.create(input).flatMap(Ok(_))
        }
    }
  }
}
