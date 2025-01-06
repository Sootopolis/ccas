package ccas

import ccas.utils.configs.AllConfigs
import zio.{Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

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

  override def run = AllConfigs.load.map { allConfig =>
    allConfig.prettyPrint()
    println(allConfig.clubs("tgbe"))
  }
}
