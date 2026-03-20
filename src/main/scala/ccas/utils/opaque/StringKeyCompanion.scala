package ccas.utils.opaque

import zio.json.{JsonFieldDecoder, JsonFieldEncoder}

trait StringKeyCompanion extends StringCompanion {
  given JsonFieldEncoder[Type] = JsonFieldEncoder.string.contramap(unwrap)
  given JsonFieldDecoder[Type] = JsonFieldDecoder.string.mapOrFail(validated)
}
