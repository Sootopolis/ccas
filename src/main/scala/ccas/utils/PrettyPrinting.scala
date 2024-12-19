package ccas.utils

import ccas.utils
import ccas.utils.PrettyPrinting.{PrettyPrinter, PrettyPrinterSetting}
import magnolia1.{AutoDerivation, CaseClass, SealedTrait}
import zio.Chunk
import zio.http.URL

import java.time.temporal.Temporal
import scala.deriving.Mirror

trait PrettyPrinting[T] {
  self: T =>
  protected val prettyPrinterSetting: PrettyPrinterSetting = PrettyPrinterSetting.default

  inline given prettyPrinter(using Mirror.Of[T]): PrettyPrinter[T] = {
    val p = PrettyPrinter.autoDerived[T]
    println("initiated")
    p
  }

  inline def prettyString(using prettyPrinter: PrettyPrinter[T]): String = {
    println(prettyPrinter.hashCode())
    prettyPrinter.prettyString(self, prettyPrinterSetting)
  }

  final def prettyPrint()(using PrettyPrinter[T]): Unit = println(prettyString)
}

object PrettyPrinting {
  def prettyString[T](t: T, setting: PrettyPrinterSetting = PrettyPrinterSetting.default)
    (using prettyPrinter: PrettyPrinter[T]): String = prettyPrinter.prettyString(t, setting)

  def prettyPrint[T](t: T, setting: PrettyPrinterSetting = PrettyPrinterSetting.default)
    (using prettyPrinter: PrettyPrinter[T]): Unit = prettyPrinter.prettyPrint(t, setting)

  case class PrettyPrinterSetting(
    nIndent       : Int = 4,
    maxWidth      : Int = 96,
    namedArguments: Boolean = true,
    trailingComma : Boolean = false
  ) {
    def indent(string: String): String = string.indent(nIndent)
  }

  object PrettyPrinterSetting {
    val default = new PrettyPrinterSetting()
  }

  trait PrettyPrinter[T] {
    def prettyString(t: T, setting: PrettyPrinterSetting): String

    def prettyPrint(t: T, setting: PrettyPrinterSetting): Unit = println(prettyString(t, setting))
  }

  object PrettyPrinter extends AutoDerivation[PrettyPrinter] {
    private def makePrinter(name: String, items: Chunk[String], setting: PrettyPrinterSetting) = {
      val header = name + "("
      val singleLine = !items.exists(_.linesIterator.drop(1).hasNext)
        && items.foldWhile(header.length)(_ < setting.maxWidth)(_ + _.length) < setting.maxWidth
      val body = {
        if (singleLine) { items.mkString(", ") }
        else if (setting.trailingComma) { "\n" + items.map(_ + ",").map(setting.indent).mkString }
        else { "\n" + items.init.map(_ + ",").map(setting.indent).mkString + items.lastOption.fold("")(setting.indent) }
      }
      header + body + ")"
    }

    override def join[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] = (t, setting) => {
      val name = caseClass.typeInfo.short
      val items = Chunk.fromIterator(caseClass.params.iterator).map { param =>
        val value = param.typeclass.prettyString(param.deref(t), setting)
        if (setting.namedArguments) { s"${ param.label } = $value" } else { value }
      }
      makePrinter(name, items, setting)
    }

    override def split[T](sealedTrait: SealedTrait[Typeclass, T]): Typeclass[T] = (t, setting) => {
      if (sealedTrait.isEnum) { t.toString }
      else { sealedTrait.choose(t) { subtype => subtype.typeclass.prettyString(subtype.cast(t), setting) } }
    }

    given collectionPrinter[T, C <: IterableOnce[T]](using prettyPrinter: PrettyPrinter[T]): PrettyPrinter[C] = {
      (collection, setting) => {
        val name = collection.getClass.getSimpleName.stripSuffix("$")
        val items = Chunk.from(collection).map(prettyPrinter.prettyString(_, setting))
        makePrinter(name, items, setting)
      }
    }

    given optionPrinter[T](using prettyPrinter: PrettyPrinter[T]): PrettyPrinter[Option[T]] = (option, setting) => {
      option.fold(None.getClass.getSimpleName.stripSuffix("$")) { t =>
        val item = prettyPrinter.prettyString(t, setting)
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
