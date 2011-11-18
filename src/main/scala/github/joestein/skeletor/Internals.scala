package github.joestein.skeletor

import java.util.Collections
import scala.collection.mutable.ListBuffer
import org.apache.cassandra.locator.SimpleStrategy
import github.joestein.skeletor.Conversions.keyspaceString
import github.joestein.util.LogHelper
import me.prettyprint.hector.api.ddl.ComparatorType
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.query.MultigetSliceQuery
import me.prettyprint.hector.api.query.{ MultigetSliceCounterQuery, CounterQuery }
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.serializers.LongSerializer
import java.lang.{ Long => JLong }

object Conversions {
    implicit def simplekey(s: String): Keyspace = Keyspace(s)
    implicit def keyspaceString(ks: Keyspace): String = ks.name
    implicit def rowString(r: Row): String = r.name
    implicit def columnfamily(cf: ColumnFamily) = cf.name
    implicit def getrows(r: Rows) = r.get
}

case class ColumnNameValue(column: Column, name: String, value: Any, isCounter: Boolean) extends LogHelper {
    def ks() = column.row.cf.ks
    def row() = column.row
    def cf() = column.row.cf

    def intValue = value match {
        case i: Int    => i
        case l: Long   => l.toInt
        case n: Number => n.intValue()
        case _         => value.toString.toInt
    }

    def hColumn = value match {
        case l: Long =>
            HFactory.createColumn(name, JLong.valueOf(l), StringSerializer.get(), LongSerializer.get())
        case _ =>
            HFactory.createStringColumn(name, value.toString)
    }
}

case class Column(row: Row, name: String) {

    def of(value: Any) = ColumnNameValue(this, name, value, false)

    def inc() = {
        ColumnNameValue(this, name, 1, true)
    }

    def inc(value: Int) = {
        ColumnNameValue(this, name, value, true)
    }

    def dec(value: Int) = {
        ColumnNameValue(this, name, (value - (2 * value)), true)
    }

    def dec() = {
        ColumnNameValue(this, name, -1, true)
    }
}

case class Row(cf: ColumnFamily, name: String) {

    def has(column: String): Column = Column(this, column)
}

object Row {
    def apply(cv: ColumnNameValue): Row = cv.row
}

class Rows(cv: Option[ColumnNameValue] = None) {
    import scala.collection.mutable.ListBuffer
    private val rows = new ListBuffer[ColumnNameValue]

    cv.foreach(rows += _)

    def add(cv: ColumnNameValue) = {
        rows += cv
        this
    }

    //need to be able to handle adding the two list buffers together 
    //without explicitly exposing the rows unecessarly
    def ++(buffRows: Rows) = {
        rows ++= buffRows.rows
        this
    }

    def get = {
        rows.result
    }
}

object Rows {
    def apply(cv: ColumnNameValue): Rows = {
        new Rows(Some(cv))
    }
}

case class ColumnFamily(val ks: Keyspace, val name: String) extends LogHelper {
    import me.prettyprint.hector.api.factory.HFactory
    import me.prettyprint.hector.api.ddl.ComparatorType
    import Conversions._

    private lazy val columnFamilyDefinition = HFactory.createColumnFamilyDefinition(ks, name, ComparatorType.UTF8TYPE)

    def ->(row: String) = new Row(this, row)

    //get data out of this column family
    def >>(sets: (MultigetSliceQuery[String, String, String]) => Unit, proc: (String, String, String) => Unit) {
        Cassandra >> (this, sets, proc)
    }

    //get data of this counter column family
    def >#(sets: (MultigetSliceCounterQuery[String, String]) => Unit, proc: (String, String, Long) => Unit) = {
        Cassandra ># (this, sets, proc)
    }

    //get data of this counter column family
    def >%(sets: (CounterQuery[String, String]) => Unit, proc: (Long) => Unit) = {
        Cassandra >% (this, sets, proc)
    }

    def <<(rows: Seq[ColumnNameValue]) = {
        Cassandra << rows
    }

    /*
     *  create the column family
     */
    def create = {
        Cassandra.cluster.addColumnFamily(columnFamilyDefinition, true)
    }

    /*
     * drop the column family from the keyspace
     */
    def delete = {
        Cassandra.cluster.dropColumnFamily(ks, name, true)
    }

    /*
     * truncate the data from this column family
     */
    def truncate = {
        Cassandra.cluster.truncate(ks, name)
    }
}

case class Keyspace(val name: String, val replicationFactor: Int = 1) {
    private lazy val keyspaceDefinition = HFactory.createKeyspaceDefinition(name, classOf[SimpleStrategy].getName(), replicationFactor, Collections.emptyList())

    def create = {
        Cassandra.cluster.addKeyspace(keyspaceDefinition, true)
    }

    def delete = {
        Cassandra.cluster.dropKeyspace(name, true)
    }

    def \(cf: String) = new ColumnFamily(this, cf)
}