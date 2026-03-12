package ccas.api.misc

import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}

@jsonMemberNames(SnakeCase)
final case class Accuracies(white: Double, black: Double) derives JsonDecoder
