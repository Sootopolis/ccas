package ccas.api.utils

import zio.json.{JsonDecoder, SnakeCase, jsonMemberNames}

@jsonMemberNames(SnakeCase)
case class Accuracies(white: Double, black: Double) derives JsonDecoder
