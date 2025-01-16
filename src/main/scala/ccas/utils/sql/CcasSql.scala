package ccas.utils.sql

import io.getquill.parser.ParserLibrary
import io.getquill.{BatchAction, Insert, Quoted, SnakeCase, Update, defaultParser, query, quote}
import zio.{Chunk, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

import java.sql.SQLException
import javax.sql.DataSource

trait CcasSql[T] {
  self =>
  val tableName: String = SnakeCase.table(self.getClass.getSimpleName)
  protected val ctx = CcasSqlContext

  import ctx.{insertValue, updateValue, given}

  inline def insert(values: IterableOnce[T]): ZIO[CcasDataSource, SQLException, Unit] =
    ctx.run[T, Insert[T]] { quote(ctx.liftQuery(Chunk.from(values)).foreach(query[T].insertValue)) }.unit

  //
  protected inline def dynamicFilter(a: T, b: T) = primaryKeys.map(key => key(a) == key(b)).reduce(_ && _)

  protected def primaryKeys: Seq[T => Any]

  inline def update(values: IterableOnce[T]): ZIO[CcasDataSource, SQLException, Unit] =
    ctx.run[T, Update[T]] {
      quote {
        ctx.liftQuery(Chunk.from(values)).foreach { value =>
          query[T].filter(row => dynamicFilter(row, value))
            .updateValue(value)
        }
      }
    }.unit

  //
  //  inline def selectAll: ZIO[CcasDataSource, Throwable, Chunk[T]]
  //
  //  inline def createTable: ZIO[CcasDataSource, Throwable, Unit]
  //
  //  inline def dropTable: ZIO[CcasDataSource, Throwable, Unit]
  //
  //  inline def clearTable: ZIO[CcasDataSource, Throwable, Unit]
}

case class Person(name: String, age: Int)

object Person extends CcasSql[Person] with ZIOAppDefault {
  override protected def primaryKeys: Seq[Person => Any] = Seq(_.name, _.age)

  override def run = {
    import ctx.given
    val persons = Seq(Person("a", 28))
    for {
      _ <- Person.insert(persons)
//      _ <- Person.update(persons)
    } yield ()
  }.provide(CcasDataSource.layer)
}
