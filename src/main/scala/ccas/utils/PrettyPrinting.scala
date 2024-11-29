package ccas.utils

import ccas.utils
import ccas.utils.PrettyPrinting.{PrettyPrinter, PrettyPrinterSetting}
import magnolia1.{AutoDerivation, CaseClass, SealedTrait}
import zio.Chunk
import zio.http.URL

import java.time.temporal.Temporal

trait PrettyPrinting[T] {
  self: T =>
  protected val prettyPrinterSetting: PrettyPrinterSetting = PrettyPrinterSetting(4, 96, false)

  def prettyPrint()(using printer: PrettyPrinter[T]): Unit = println(printer.prettyPrint(self, prettyPrinterSetting))
}

object PrettyPrinting {
  case class PrettyPrinterSetting(nIndent: Int, maxWidth: Int, multilineTrailingComma: Boolean) {
    def indent(string: String): String = string.indent(nIndent)
  }

  trait PrettyPrinter[T] {
    def prettyPrint(t: T, setting: PrettyPrinterSetting): String
  }

  object PrettyPrinter extends AutoDerivation[PrettyPrinter] {
    private def makePrinter(name: String, items: Chunk[String], setting: PrettyPrinterSetting) = {
      val header = name + "("
      val singleLine = !items.exists(_.linesIterator.drop(1).hasNext)
        && items.foldWhile(header.length)(_ < setting.maxWidth)(_ + _.length) < setting.maxWidth
      val body = {
        if (singleLine) { items.mkString(", ") }
        else if (setting.multilineTrailingComma) { "\n" + items.map(_ + ",").map(setting.indent).mkString }
        else { "\n" + items.init.map(_ + ",").map(setting.indent).mkString + items.lastOption.fold("")(setting.indent) }
      }
      header + body + ")"
    }

    override def join[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] = (t, setting) => {
      val name = caseClass.typeInfo.short
      val items = Chunk.from(caseClass.params)
        .map { param => s"${ param.label } = ${ param.typeclass.prettyPrint(param.deref(t), setting) }" }
      makePrinter(name, items, setting)
    }

    override def split[T](sealedTrait: SealedTrait[Typeclass, T]): Typeclass[T] = (t, setting) => {
      if (sealedTrait.isEnum) { t.toString }
      else { sealedTrait.choose(t) { subtype => subtype.typeclass.prettyPrint(subtype.cast(t), setting) } }
    }

    given collectionPrinter[T, C <: IterableOnce[T]](using pp: PrettyPrinter[T]): PrettyPrinter[C] = (c, setting) => {
      val name = c.getClass.getSimpleName.stripSuffix("$")
      val items = Chunk.from(c).map(pp.prettyPrint(_, setting))
      makePrinter(name, items, setting)
    }

    given optionPrinter[T](using pp: PrettyPrinter[T]): PrettyPrinter[Option[T]] = (option, setting) => {
      option.fold(None.getClass.getSimpleName.stripSuffix("$")) { t =>
        val item = pp.prettyPrint(t, setting)
        val body = if (item.linesIterator.drop(1).hasNext) { setting.indent(item) } else { item }
        classOf[Some[?]].getSimpleName + "(" + body + ")"
      }
    }

    given stringPrinter[T <: String]: PrettyPrinter[T] = (t, _) => t

    given anyValPrinter[T <: AnyVal]: PrettyPrinter[T] = (t, _) => t.toString

    given temporalPrinter[T <: Temporal]: PrettyPrinter[T] = (t, _) => t.toString

    given urlPrinter: PrettyPrinter[URL] = (url, _) => url.encode
  }
}
