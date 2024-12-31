package ccas

import zio.json.{DeriveJsonCodec, EncoderOps, JsonCodec, jsonAliases}

object Playground extends App {
  enum Animal {
    @jsonAliases("persian") case Cat
    case Dog
  }

  given JsonCodec[Animal] = DeriveJsonCodec.gen

  println(Animal.Cat.toJsonPretty)
}
