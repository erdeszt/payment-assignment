package challenge

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import cats.effect.Blocker
import cats.effect.IO
import cats.instances.list._
import cats.Traverse.ops._
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import org.http4s.Uri
import org.http4s.client.dsl.io._
import org.http4s.dsl.io._
import org.http4s.implicits._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ChallengeTest extends AnyFreeSpec with Matchers {
  val blockingPool = Executors.newFixedThreadPool(5)
  val blocker = Blocker.liftExecutorService(blockingPool)
  implicit val cs = IO.contextShift(ExecutionContexts.synchronous)
  implicit val timer = IO.timer(scala.concurrent.ExecutionContext.Implicits.global) // TODO

  val config = new HikariConfig()
  config.setDriverClassName("com.mysql.cj.jdbc.Driver")
  config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/challenge?serverTimezone=UTC")
  config.setUsername("root")
  config.setPassword("root")

  // Use this Transactor with the HikariConfig above for MySQL
  val tx: HikariTransactor[IO] = HikariTransactor(new HikariDataSource(config), global, blocker)

  val payersService = Payers.routes(Payers.impl(tx))
  val invoicesService = Invoices.routes(Invoices.impl(tx))
  val paymentsService = Payments.routes(Payments.impl(tx))

  "Payers" - {
    "should be creatable" in {
      val io = for {
        createReq <- POST(Payers.New("Mr Jameson"), uri"""http://0.0.0.0:8080/payer""")
        payerId <- payersService.orNotFound.run(createReq).flatMap(_.as[Payers.Id])

        fetchReq <- GET(uri"""http://0.0.0.0:8080/payer/""".addSegment(payerId.id.toString))
        payer <- payersService.orNotFound.run(fetchReq).flatMap(_.as[Payers.Payer])
      } yield payer should matchPattern { case Payers.Payer(id, _) if id == payerId.id => }

      io.unsafeRunSync()
    }

    "should have the right balance after adding invoices and payments" in {
      val io = for {
        payerReq <- POST(Payers.New("Mrs Brodie"), uri"""http://0.0.0.0:8080/payer""")
        payerId <- payersService.orNotFound.run(payerReq).flatMap(_.as[Payers.Id])

        // Add invoice of -100
        invReq1 <- POST(
          Invoices.New(-100, payerId.id, Some(LocalDateTime.of(2020, 10, 10, 14, 30))),
          uri"""http://0.0.0.0:8080/invoice"""
        )
        _ <- invoicesService.orNotFound.run(invReq1).flatMap(_.as[Unit])

        // Add payment of 50
        paymentReq <- POST(
          Payments.New(50, payerId.id, Some(LocalDateTime.of(2020, 10, 10, 14, 45))),
          uri"""http://0.0.0.0:8080/payment"""
        )
        _ <- paymentsService.orNotFound.run(paymentReq).flatMap(_.as[Unit])

        // Add invoice of -100
        invReq2 <- POST(
          Invoices.New(-100, payerId.id, Some(LocalDateTime.of(2020, 10, 10, 17, 30))),
          uri"""http://0.0.0.0:8080/invoice"""
        )
        _ <- invoicesService.orNotFound.run(invReq2).flatMap(_.as[Unit])

        // Add invoice of -250
        invReq3 <- POST(
          Invoices.New(-250, payerId.id, Some(LocalDateTime.of(2020, 10, 11, 11, 30))),
          uri"""http://0.0.0.0:8080/invoice"""
        )
        _ <- invoicesService.orNotFound.run(invReq3).flatMap(_.as[Unit])

        // Add payment of 100
        paymentReq <- POST(
          Payments.New(100, payerId.id, Some(LocalDateTime.of(2020, 10, 12, 11, 27))),
          uri"""http://0.0.0.0:8080/payment"""
        )
        _ <- paymentsService.orNotFound.run(paymentReq).flatMap(_.as[Unit])

        // Get balance on 2020-10-11
        targetDate = LocalDate.of(2020, 10, 11).format(DateTimeFormatter.ISO_DATE)
        balancePath <- IO.fromEither(Uri.fromString(s"http://0.0.0.0:8080/payer/${payerId.id}/balance/$targetDate"))
        balanceReq <- GET(balancePath)
        balance <- payersService.orNotFound.run(balanceReq).flatMap(_.as[Payers.Balance])
      } yield assertResult(-150)(balance.balance)

      io.unsafeRunSync()
    }

    "should have the right balance for all users" in {
      val io = for {
        payer1Req <- POST(Payers.New("Mrs Brodie"), uri"""http://0.0.0.0:8080/payer""")
        payer1Id <- payersService.orNotFound.run(payer1Req).flatMap(_.as[Payers.Id])
        payer2Req <- POST(Payers.New("Ms Ferrara"), uri"""http://0.0.0.0:8080/payer""")
        payer2Id <- payersService.orNotFound.run(payer2Req).flatMap(_.as[Payers.Id])

        // Add invoices of -100 for both payers
        invReq1 <- POST(
          Invoices.New(-100, payer1Id.id, Some(LocalDateTime.of(2020, 10, 10, 14, 30))),
          uri"""http://0.0.0.0:8080/invoice"""
        )
        _ <- invoicesService.orNotFound.run(invReq1).flatMap(_.as[Unit])
        invReq2 <- POST(
          Invoices.New(-100, payer2Id.id, Some(LocalDateTime.of(2020, 10, 10, 14, 30))),
          uri"""http://0.0.0.0:8080/invoice"""
        )
        _ <- invoicesService.orNotFound.run(invReq2).flatMap(_.as[Unit])

        // Add payment of 50 for payer 1
        payment1Req <- POST(
          Payments.New(50, payer1Id.id, Some(LocalDateTime.of(2020, 10, 10, 14, 45))),
          uri"""http://0.0.0.0:8080/payment"""
        )
        _ <- paymentsService.orNotFound.run(payment1Req).flatMap(_.as[Unit])

        // Add payment of 150 for payer 2
        payment2Req <- POST(
          Payments.New(150, payer2Id.id, Some(LocalDateTime.of(2020, 10, 10, 14, 45))),
          uri"""http://0.0.0.0:8080/payment"""
        )
        _ <- paymentsService.orNotFound.run(payment2Req).flatMap(_.as[Unit])

        balancesReq <- GET(uri"""http://0.0.0.0:8080/payer""")
        balances <- payersService.orNotFound.run(balancesReq).flatMap(_.as[Payers.Balances])
        balanceLookup = balances.balances.map(balance => (balance.payerId, balance.balance)).toMap
      } yield {
        balanceLookup.get(payer1Id.id) should contain(-50)
        balanceLookup.get(payer2Id.id) should contain(50)
      }

      io.unsafeRunSync()
    }
  }

  "Invoices" - {
    "should be creatable" in {
      val io = for {
        payerReq <- POST(Payers.New("Ms Ferrara"), uri"""http://0.0.0.0:8080/payer""")
        payerId <- payersService.orNotFound.run(payerReq).flatMap(_.as[Payers.Id])

        invCreateReq <- POST(Invoices.New(100, payerId.id, None), uri"""http://0.0.0.0:8080/invoice""")
        invoiceId <- invoicesService.orNotFound.run(invCreateReq).flatMap(_.as[Invoices.Id])

        invFetchReq <- GET(uri"""http://0.0.0.0:8080/invoice""".addSegment(invoiceId.id.toString))
        invoice <- invoicesService.orNotFound.run(invFetchReq).flatMap(_.as[Invoices.Invoice])
      } yield invoice should matchPattern { case Invoices.Invoice(id, _, _, _) if id == invoiceId.id => }

      io.unsafeRunSync()
    }
  }

  "Payments" - {
    "should be creatable" in {
      val io = for {
        payerReq <- POST(Payers.New("Dr Theodore"), uri"""http://0.0.0.0:8080/payer""")
        payerId <- payersService.orNotFound.run(payerReq).flatMap(_.as[Payers.Id])

        paymentCreateReq <- POST(Payments.New(75, payerId.id, None), uri"""http://0.0.0.0:8080/payment""")
        paymentId <- paymentsService.orNotFound.run(paymentCreateReq).flatMap(_.as[Payments.Id])

        paymentFetchReq <- GET(uri"""http://0.0.0.0:8080/payment""".addSegment(paymentId.id.toString))
        payment <- paymentsService.orNotFound.run(paymentFetchReq).flatMap(_.as[Payments.Payment])
      } yield payment should matchPattern { case Payments.Payment(id, _, _, _) if id == paymentId.id => }

      io.unsafeRunSync()
    }

    "should find the correct invoices covered by a payment" in {
      val io = for {
        payerReq <- POST(Payers.New("Dr Theodore"), uri"""http://0.0.0.0:8080/payer""")
        payerId <- payersService.orNotFound.run(payerReq).flatMap(_.as[Payers.Id])

        // Add 3 invoices of -100
        // NOTE: Unsafe match but it's fine for tests
        (invoice1Id :: invoice2Id :: invoice3Id :: Nil) <- 1.to(3).toList.traverse { index =>
          POST(
            Invoices.New(-100, payerId.id, Some(LocalDateTime.of(2020, 10, index, 14, 30))),
            uri"""http://0.0.0.0:8080/invoice"""
          ).flatMap(req => invoicesService.orNotFound.run(req).flatMap(_.as[Invoices.Id]))
        }

        // Add 3 payments of 150, 200 and 50
        (payment1Id :: payment2Id :: payment3Id :: Nil) <- List(150, 200, 50).zipWithIndex.traverse {
          case (amount, index) =>
            POST(
              Payments.New(amount, payerId.id, Some(LocalDateTime.of(2020, 11, index + 1, 14, 45))),
              uri"""http://0.0.0.0:8080/payment"""
            ).flatMap(req => paymentsService.orNotFound.run(req).flatMap(_.as[Payments.Id]))
        }

        // Get the coverage for all three payments
        (payment1Covers :: payment2Covers :: payment3Covers :: Nil) <- List(payment1Id, payment2Id, payment3Id)
          .traverse { paymentId =>
            for {
              coversUrl <- IO.fromEither(Uri.fromString(s"http://0.0.0.0:8080/payment/${paymentId.id}/covers"))
              coversReq <- GET(coversUrl)
              covers <- paymentsService.orNotFound.run(coversReq).flatMap(_.as[Payments.Covers])
            } yield covers
          }
      } yield {
        payment1Covers.invoices should contain theSameElementsAs List(invoice1Id, invoice2Id)
        payment2Covers.invoices should contain theSameElementsAs List(invoice2Id, invoice3Id)
        payment3Covers.invoices should be(empty)
      }

      io.unsafeRunSync()
    }
  }
}
