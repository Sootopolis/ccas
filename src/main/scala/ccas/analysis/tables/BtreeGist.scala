package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*

/** The name tables' exclusion constraints need `btree_gist`, which is installed by hand rather than from the boot path
  * (ADR 0016), so a missing extension fails boot with a pointer to the runbook instead.
  */
private[tables] object BtreeGist {

  def require(table: String)(using DbCon): Unit = {
    val installed =
      sql"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'btree_gist')".query[Boolean].run().head
    if (!installed) {
      throw SQLException(
        s"$table needs the btree_gist extension: run 'CREATE EXTENSION btree_gist;' against this database " +
          "(docs/adr/0016-identity-is-the-id-names-are-observations.md)"
      )
    }
  }
}
