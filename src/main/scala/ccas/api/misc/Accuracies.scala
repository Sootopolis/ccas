package ccas.api.misc

import zio.json.{JsonDecoder, SnakeCase, jsonMemberNames}

@jsonMemberNames(SnakeCase)
final case class Accuracies(white: Double, black: Double) derives JsonDecoder
