package ccas

import ccas.utils.configs.{AllConfigs, ClubConfig, RecruitmentConfig}
import ccas.utils.prettyprinting.PrettyPrinter
import zio.{Chunk, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

object Playground extends ZIOAppDefault {
//  case class Person(firstName: String, lastName: String, age: Int, url: URL, instant: Instant)
//
//  object Person {
//    def initialiseDatabase = context.JdbcDecoder
//  }
//
//  val context = new CcasSqlContext
//
//  import context.{*, given}
//
//  val named = "Joe"
//
//  inline def somePeople: Quoted[EntityQuery[Person]] = quote {
//    query[Person].filter(_.instant > context.lift(Instant.EPOCH))
//  }

  case class Thing(chunk: Chunk[(String, Int)]) derives PrettyPrinter
  override def run = for {
    _ <- AllConfigs.load.map(_.prettyPrint())
    _ <- ClubConfig.load("devon").map(_.prettyPrint())
    _ <- ZIO.succeed(Thing(Chunk("a" -> 1, "b" -> 2)).prettyPrint())
  } yield ()
}
