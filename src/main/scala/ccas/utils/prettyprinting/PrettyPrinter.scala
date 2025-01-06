package ccas.utils.prettyprinting

import ccas.utils.prettyprinting.PrettyPrinter.PrettyPrinterSetting
import magnolia1.{AutoDerivation, CaseClass, SealedTrait}
import zio.Chunk
import zio.http.URL

import java.time.temporal.Temporal
import scala.reflect.{ClassTag, classTag}

/** Enable pretty printing of a case class by adding `derives PrettyPrinter` after the round brackets. */
trait PrettyPrinter[T] {
  protected def getPrettyString(value: T, setting: PrettyPrinterSetting, className: String): String

  protected val prettyPrinterSetting: PrettyPrinterSetting = PrettyPrinterSetting.default

  extension (value: T)(using classTag: ClassTag[T], prettyPrinter: PrettyPrinter[T]) {
    def toPrettyString(setting: PrettyPrinterSetting = prettyPrinterSetting): String =
      prettyPrinter.getPrettyString(value, setting, classTag.getClass.getSimpleName)

    def toPrettyString: String = toPrettyString(prettyPrinterSetting)

    def prettyPrint(setting: PrettyPrinterSetting = prettyPrinterSetting): Unit = println(toPrettyString(setting))
  }
}

object PrettyPrinter extends AutoDerivation[PrettyPrinter] {
  case class PrettyPrinterSetting(
    nIndent       : Int = 2,
    maxWidth      : Int = 64,
    namedArguments: Boolean = true,
    trailingComma : Boolean = false,
    ignoreDefault : Boolean = false,
  ) {
    def indent(string: String): String = string.indent(nIndent)
  }

  object PrettyPrinterSetting {
    val default = new PrettyPrinterSetting()
  }

  private def makePrettyString(name: String, items: Chunk[String], setting: PrettyPrinterSetting) = {
    val header = name + "("
    val footer = ")"
    val isSingleLine = !items.exists(_.linesIterator.drop(1).hasNext)
      && items.foldWhile(header.length)(_ < setting.maxWidth)(_ + _.length) < setting.maxWidth
    val body = {
      if (isSingleLine) { items.mkString(", ") }
      else if (setting.trailingComma) { "\n" + items.map(_ + ",").map(setting.indent).mkString }
      else { "\n" + items.init.map(_ + ",").map(setting.indent).mkString + items.lastOption.fold("")(setting.indent) }
    }
    header + body + footer
  }

  override def join[T](caseClass: CaseClass[PrettyPrinter, T]): PrettyPrinter[T] = new PrettyPrinter[T] {
    override protected def getPrettyString(value: T, setting: PrettyPrinterSetting, className: String): String = {
      if (caseClass.isObject) { caseClass.typeInfo.short } else {
        val name = caseClass.typeInfo.short
        val fields = Class.forName(caseClass.typeInfo.full).getDeclaredFields.map(field => field.getName -> field).toMap
        val items = Chunk.fromIterator(caseClass.params.iterator).flatMap { param =>
          val dereferenced = param.deref(value)
          Option.unless(setting.ignoreDefault && param.default.contains(dereferenced)) {
            val paramClassName = fields(param.label).getType.getSimpleName
            val prettyValue = param.typeclass.getPrettyString(param.deref(value), setting, paramClassName)
            if (setting.namedArguments) { s"${ param.label } = $prettyValue" } else { prettyValue }
          }
        }
        makePrettyString(name, items, setting)
      }
    }
  }

  override def split[T](sealedTrait: SealedTrait[PrettyPrinter, T]): PrettyPrinter[T] = new PrettyPrinter[T] {
    override protected def getPrettyString(value: T, setting: PrettyPrinterSetting, className: String): String = {
      if (sealedTrait.isEnum) { value.toString } else {
        sealedTrait.choose(value) { sub => sub.typeclass.getPrettyString(sub.cast(value), setting, sub.typeInfo.short) }
      }
    }
  }

  given collectionPrinter[T: ClassTag, C <: IterableOnce[T]](using pp: PrettyPrinter[T]): PrettyPrinter[C] with {
    private val tClassName = classTag[T].runtimeClass.getSimpleName

    override protected def getPrettyString(value: C, setting: PrettyPrinterSetting, className: String): String = {
      val name = className
      val items = Chunk.from(value).map(pp.getPrettyString(_, setting, tClassName))
      makePrettyString(name, items, setting)
    }
  }

  given mapPrinter[K: ClassTag, V: ClassTag, M <: Map[K, V]]
  (using kpp: PrettyPrinter[K], vpp: PrettyPrinter[V]): PrettyPrinter[M] with {
    private val kClassName = classTag[K].runtimeClass.getSimpleName
    private val vClassName = classTag[V].runtimeClass.getSimpleName

    override protected def getPrettyString(value: M, setting: PrettyPrinterSetting, className: String): String = {
      val name = className
      val items = Chunk.from(value).map { (k, v) =>
        kpp.getPrettyString(k, setting, kClassName) + " -> " + vpp.getPrettyString(v, setting, vClassName)
      }
      makePrettyString(name, items, setting)
    }
  }

  given optionPrinter[T: ClassTag](using prettyPrinter: PrettyPrinter[T]): PrettyPrinter[Option[T]] with {
    private val tClassName = classTag[T].runtimeClass.getSimpleName

    override protected def getPrettyString(
      value    : Option[T],
      setting  : PrettyPrinterSetting,
      className: String
    ): String = value.fold(None.getClass.getSimpleName.stripSuffix("$")) { t =>
      val item = prettyPrinter.getPrettyString(t, setting, tClassName)
      makePrettyString(Some.getClass.getSimpleName.stripSuffix("$"), Chunk(item), setting)
    }
  }

  given stringPrinter[T <: String]: PrettyPrinter[T] = (value, _, _) => s"\"$value\""

  given booleanPrinter[T <: Boolean]: PrettyPrinter[T] = (value, _, _) => value.toString

  given intPrinter[T <: Int]: PrettyPrinter[T] = (value, _, _) => value.toString

  given longPrinter[T <: Long]: PrettyPrinter[T] = (value, _, _) => value.toString

  given shortPrinter[T <: Short]: PrettyPrinter[T] = (value, _, _) => value.toString

  given floatPrinter[T <: Float]: PrettyPrinter[T] = (value, _, _) => value.toString

  given doublePrinter[T <: Double]: PrettyPrinter[T] = (value, _, _) => value.toString

  given temporalPrinter[T <: Temporal]: PrettyPrinter[T] = (value, _, _) => value.toString

  given urlPrinter: PrettyPrinter[URL] = (value, _, _) => value.encode
}
