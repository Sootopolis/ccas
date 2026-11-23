package ccas.rules

import scala.annotation.tailrec
import scala.meta.*

import metaconfig.{Conf, Configured}
import scalafix.v1.*

import ccas.rules.PackageLayers.FilePackage

/** A package imports only from its own layer and the layers below it. `PackageLayers.layers` in `.scalafix.conf` lists
  * the layers bottom first, each a package prefix, and a name belongs to the longest prefix it falls under. The rule
  * reads a file's `package` clause, its imports and any name selected from `ccas`. A relative name stays in the file's
  * package, except where that package holds another layer: in `ccas.analysis`, `apps.X` is `ccas.analysis.apps.X`,
  * and is read so, as is a local value named `apps` there, which wants renaming. A file keeps to one package clause:
  * nested clauses open every enclosing package to relative names, and a second block would hide another layer's code.
  * A `ccas` package no layer covers is reported, so a new package gets a place before anything depends on it.
  * Scalafix reads source, which still names an opaque type that bytecode erases. See
  * docs/adr/0025-conventions-checked-by-scalafix.md.
  */
class PackageLayers(layers: List[String]) extends SyntacticRule("PackageLayers") {

  def this() = this(Nil)

  // Only an explicit `layers = []` turns the rule off: a missing or misspelt key would otherwise pass every file.
  override def withConfiguration(config: Configuration): Configured[Rule] =
    config.conf
      .getOrElse[Conf]("PackageLayers")(Conf.Obj.empty)
      .andThen(_.getOrElse[Option[List[String]]]("layers")(None))
      .andThen {
        case Some(layers) => Configured.ok[Rule](new PackageLayers(layers))
        case None => Configured.error("PackageLayers.layers is missing from .scalafix.conf; `layers = []` turns it off")
      }

  override def fix(implicit doc: SyntacticDocument): Patch =
    filePackage(doc.tree) match {
      case Some(file) if layers.nonEmpty && isOurs(file.name) =>
        if (!file.single) {
          lint(
            file.ref.pos,
            "Keep this file to one package clause: nested clauses open each enclosing package to relative names, " +
              "and a second block hides another layer's code, neither of which PackageLayers can follow"
          )
        } else {
          layerOf(file.name).fold(unplaced(file.name, file.ref.pos)) { own =>
            references(doc.tree, file.name).map { case (target, tree) => check(own, target, tree.pos) }.asPatch
          }
        }
      case _ => Patch.empty
    }

  private def check(own: Int, target: String, position: Position): Patch =
    layerOf(target) match {
      case None if target == "ccas" =>
        lint(position, "ccas itself holds every layer: import from a package inside it")
      case None => unplaced(target, position)
      case Some(layer) if layer > own =>
        lint(
          position,
          s"$target is in ${layers(layer)}, above ${layers(own)}: a package imports only from its own " +
            "layer and those below it (PackageLayers.layers in .scalafix.conf)"
        )
      case Some(_) => Patch.empty
    }

  private def unplaced(name: String, position: Position): Patch =
    lint(position, s"$name is in no layer: give it one in PackageLayers.layers (.scalafix.conf)")

  private def lint(position: Position, message: String): Patch =
    Patch.lint(Diagnostic(id = "", message = message, position = position))

  private def filePackage(tree: Tree): Option[FilePackage] = {
    @tailrec
    def descend(stats: List[Stat], clauses: List[Term.Ref], siblings: Boolean): Option[FilePackage] =
      stats.collect { case pkg: Pkg => pkg } match {
        case pkg :: rest => descend(pkg.body.stats, clauses :+ pkg.ref, siblings || rest.nonEmpty)
        case Nil =>
          clauses.lastOption.map { ref =>
            val name = clauses.map(dotted).mkString(".")
            FilePackage(name = name, ref = ref, single = clauses.length == 1 && !siblings)
          }
      }
    tree match {
      case source: Source => descend(source.stats, Nil, siblings = false)
      case _              => None
    }
  }

  // Each import names its targets: `a.b.X` for a name or rename, `a.b` for a wildcard or given. Elsewhere, a chain
  // selected from `ccas`, or from a layer nested in the file's package, is its own target.
  private def references(tree: Tree, pkg: String): List[(String, Tree)] = {
    val nested = layers.collect {
      case layer if layer.startsWith(s"$pkg.") => layer.stripPrefix(s"$pkg.").takeWhile(_ != '.')
    }.toSet
    def absolute(path: String): Option[String] =
      if (path.startsWith("_root_.")) { Some(path.stripPrefix("_root_.")).filter(isOurs) }
      else if (isOurs(path)) { Some(path) }
      else { Option.when(nested.contains(path.takeWhile(_ != '.')))(s"$pkg.$path") }
    tree.collect {
      case importer: Importer =>
        absolute(dotted(importer.ref)).toList.flatMap(importTargets(_, importer.importees)).map(_ -> importer)
      case name @ Term.Name(value) if (value == "ccas" || nested.contains(value)) && isRoot(name) =>
        val chain    = top(name)
        val inClause = chain.parent.exists(parent => parent.is[Importer] || parent.is[Pkg])
        // A bare name selects nothing, so it is a local value rather than a package.
        if ((chain eq name) || inClause) { Nil }
        else { absolute(dotted(chain)).map(_ -> chain).toList }
    }.flatten
  }

  private def importTargets(base: String, importees: List[Importee]): List[String] =
    importees.collect {
      case Importee.Name(name)                                             => s"$base.${name.value}"
      case Importee.Rename(name, _)                                        => s"$base.${name.value}"
      case _: Importee.Wildcard | _: Importee.Given | _: Importee.GivenAll => base
    }

  private def isRoot(name: Term.Name): Boolean =
    name.parent match {
      case Some(select: Term.Select) if select.name eq name => isRootPrefix(select.qual)
      case _                                                => true
    }

  private def isRootPrefix(qual: Term): Boolean =
    qual match {
      case Term.Name("_root_") => true
      case _                   => false
    }

  @tailrec
  private def top(tree: Tree): Tree =
    tree.parent match {
      case Some(select: Term.Select) if select.qual eq tree                                => top(select)
      case Some(select: Term.Select) if (select.name eq tree) && isRootPrefix(select.qual) => top(select)
      case Some(select: Type.Select) if select.qual eq tree                                => select
      case _                                                                               => tree
    }

  private def layerOf(name: String): Option[Int] =
    layers.zipWithIndex.filter { case (layer, _) => name == layer || name.startsWith(s"$layer.") }
      .maxByOption { case (layer, _) => layer.length }
      .map { case (_, index) => index }

  private def isOurs(name: String): Boolean = name == "ccas" || name.startsWith("ccas.")

  private def dotted(tree: Tree): String = tree.syntax.filterNot(_.isWhitespace)
}

object PackageLayers {
  private[rules] final case class FilePackage(name: String, ref: Term.Ref, single: Boolean)
}
