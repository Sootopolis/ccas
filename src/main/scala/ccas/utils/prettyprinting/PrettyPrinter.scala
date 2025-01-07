package ccas.utils.prettyprinting

import ccas.utils.prettyprinting.PrettyPrinter.Setting
import magnolia1.{AutoDerivation, CaseClass, SealedTrait}
import zio.Chunk
import zio.http.URL

import java.time.temporal.Temporal
import scala.deriving.Mirror
import scala.reflect.{ClassTag, classTag}

/** Enable pretty printing of a case class by adding `derives PrettyPrinter` after the round brackets. */
trait PrettyPrinter[T] { self =>
  protected def getPrettyString(value: T, setting: Setting, className: String): String

  protected val overrideSetting: Option[Setting] = None

  private def getSettingOrElse(default: Setting) = overrideSetting.getOrElse(default)

  extension (value: T)(using tClassTag: ClassTag[T]) {
    def toPrettyString(overrideSettingOption: Option[Setting] = None): String = getPrettyString(
      value = value,
      setting = overrideSettingOption.orElse(overrideSetting).getOrElse(Setting.default),
      className = tClassTag.runtimeClass.getName
    )

    def toPrettyString: String = toPrettyString(None)

    def prettyPrint(settingOption: Option[Setting] = None): Unit =
      println(toPrettyString(settingOption))
  }
}

object PrettyPrinter extends AutoDerivation[PrettyPrinter] {
  /**
   * Settings for PrettyPrinter[T].
   * @param nIndent Indentation length (default: 2).
   * @param maxWidth Max width for a single-line element (default: 80).
   * @param namedArguments Whether arguments are to be named (default: true).
   * @param trailingComma Whether to have a comma after the last element in a multi-line element (default: false).
   * @param ignoreDefault Whether to omit elements equal to their default values (default: false).
   */
  case class Setting(
    nIndent       : Int = 2,
    maxWidth      : Int = 80,
    namedArguments: Boolean = true,
    trailingComma : Boolean = false,
    ignoreDefault : Boolean = false,
  ) {
    def indent(string: String): String = string.indent(nIndent)
  }

  object Setting {
    val default = new Setting()
  }

  private def makePrettyString(name: String, items: Chunk[String], setting: Setting) = {
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
    override protected def getPrettyString(value: T, setting: Setting, className: String): String = {
      if (caseClass.isObject) { caseClass.typeInfo.short } else {
        println(caseClass.typeInfo)
        val name = caseClass.typeInfo.short
        val fields = Class.forName(className).getDeclaredFields.map(field => field.getName -> field).toMap
        val items = Chunk.fromIterator(caseClass.params.iterator).flatMap { param =>
          val dereferenced = param.deref(value)
          Option.unless(setting.ignoreDefault && param.default.contains(dereferenced)) {
            val paramClassName = fields(param.label).getType.getName
            val paramPrettyPrinter = param.typeclass
            val paramSetting = paramPrettyPrinter.getSettingOrElse(setting)
            val prettyValue = paramPrettyPrinter.getPrettyString(param.deref(value), paramSetting, paramClassName)
            if (setting.namedArguments) { s"${ param.label } = $prettyValue" } else { prettyValue }
          }
        }
        makePrettyString(name, items, setting)
      }
    }
  }

  override def split[T](sealedTrait: SealedTrait[PrettyPrinter, T]): PrettyPrinter[T] = new PrettyPrinter[T] {
    override protected def getPrettyString(value: T, setting: Setting, className: String): String = {
      if (sealedTrait.isEnum) { value.toString } else {
        sealedTrait.choose(value) { subtype =>
          val subtypeSetting = subtype.typeclass.getSettingOrElse(setting)
          subtype.typeclass.getPrettyString(subtype.cast(value), subtypeSetting, subtype.typeInfo.short)
        }
      }
    }
  }

  given collectionPrinter[T: ClassTag, C[A] <: IterableOnce[A]](using pp: PrettyPrinter[T]): PrettyPrinter[C[T]] with {
    override protected def getPrettyString(value: C[T], setting: Setting, className: String): String = {
      val name = className.split(Array('$', '.')).last
      val tClassName = classTag[T].runtimeClass.getName
      val items = Chunk.from(value).map(pp.getPrettyString(_, pp.getSettingOrElse(setting), tClassName))
      makePrettyString(name, items, setting)
    }
  }

  given mapPrinter[K: ClassTag, V: ClassTag, M[A, B] <: collection.Map[A, B]]
    (using kpp: PrettyPrinter[K], vpp: PrettyPrinter[V], mct: ClassTag[M[K, V]]): PrettyPrinter[M[K, V]] with {
    override protected def getPrettyString(value: M[K, V], setting: Setting, className: String): String = {
      println(mct.runtimeClass.getName)
      val name = className.split(Array('$', '.')).last
      val kClassName = classTag[K].runtimeClass.getName
      val vClassName = classTag[V].runtimeClass.getName
      val items = Chunk.from(value).map { (k, v) =>
        kpp.getPrettyString(k, kpp.getSettingOrElse(setting), kClassName)
          + " -> " + vpp.getPrettyString(v, vpp.getSettingOrElse(setting), vClassName)
      }
      makePrettyString(name, items, setting)
    }
  }

  given optionPrinter[T: ClassTag](using prettyPrinter: PrettyPrinter[T]): PrettyPrinter[Option[T]] with {
    override protected def getPrettyString(value: Option[T], setting: Setting, className: String): String = {
      val tClassName = classTag[T].runtimeClass.getName
      value.fold("None") { t =>
        val item = prettyPrinter.getPrettyString(t, prettyPrinter.getSettingOrElse(setting), tClassName)
        makePrettyString("Some", Chunk(item), setting)
      }
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

  given emptyTuplePrinter: PrettyPrinter[EmptyTuple] with {
    override protected def getPrettyString(value: EmptyTuple, setting: Setting, className: String): String =
      EmptyTuple.toString()
  }

  given nonEmptyTuplePrinter[H: ClassTag, T <: Tuple]
    (using hpp: PrettyPrinter[H], tpp: PrettyPrinter[T]): PrettyPrinter[H *: T] with {
    override protected def getPrettyString(value: H *: T, setting: Setting, className: String): String = {
      val headClassName = classTag[H].runtimeClass.getName
      val printedHead = hpp.getPrettyString(value.head, setting, headClassName)
      val printedTail = tpp.getPrettyString(value.tail, setting, Tuple.getClass.getName)
      if (printedTail == EmptyTuple.toString()) { s"($printedHead)" } else { s"($printedHead, ${ printedTail.tail }" }
    }
  }

  private def setOverrideSetting[T](prettyPrinter: PrettyPrinter[T], settingOption: Option[Setting]) = {
    if (prettyPrinter.overrideSetting == settingOption) { prettyPrinter } else {
      new PrettyPrinter[T] {
        override protected val overrideSetting = settingOption

        override protected def getPrettyString(value: T, setting: Setting, className: String): String =
          prettyPrinter.getPrettyString(value, setting, className)
      }
    }
  }

  inline def derived[T](setting: Setting)(using Mirror.Of[T]): PrettyPrinter[T] =
    setOverrideSetting(derivedMirror[T], Some(setting))
}
