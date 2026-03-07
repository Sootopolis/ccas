package ccas.utils.opaque

trait IntCompanion[I] {
  protected def fromIntUnsafe(int: Int): I
  protected def toIntUnsafe(opaque: I): Int
}
