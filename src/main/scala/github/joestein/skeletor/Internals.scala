package github.joestein.skeletor

import github.joestein.util.{LogHelper}
import me.prettyprint.hector.api.query.{MultigetSliceQuery,MultigetSliceCounterQuery,CounterQuery}
import me.prettyprint.hector.api.ddl.{ComparatorType}

object Conversions {
	implicit def simplekey(s: String): Keyspace = Keyspace(s)
	implicit def keyspaceString(ks: Keyspace): String = ks.name
	implicit def rowString(r: Row): String = r.name
	implicit def columnfamily(cf: ColumnFamily) = cf.name
	implicit def getrows(r: Rows) = r.rows
}

case class ColumnNameValue(column: Column, name: String, value: String, isCounter: Boolean) {
	def ks() = column.row.cf.ks
	def row() = column.row
	def cf() = column.row.cf
}

case class Column(row: Row, name: String) {

	def of(value: String) = {
		ColumnNameValue(this,name,value.toString(),false)
	}
	
	def inc() = {
		ColumnNameValue(this,name,"1",true)
	}
	
	def inc(value: Int) = {
		ColumnNameValue(this,name,value.toString(),true)
	}	
	
	def dec(value: Int) = {
		ColumnNameValue(this,name,(value-(2*value)).toString(),true)
	}	
	
	def dec() = {
		ColumnNameValue(this,name,"-1",true)
	}
}

case class Row(cf: ColumnFamily, name: String) {

	def has(column:String):Column = Column(this,column)
}

object Row {
	def apply(cv:ColumnNameValue):Row = cv.row
}

class Rows {
	import scala.collection.mutable.ListBuffer
	var rows:ListBuffer[ColumnNameValue] = new ListBuffer[ColumnNameValue]
	def add(cv:ColumnNameValue) = rows.append(cv)
	
	//need to be able to handle adding the two list buffers together 
	//without explicitly exposing the rows unecessarly
	def ++(buffRows: Rows) = {
		rows = rows ++ buffRows.rows
	}	
}

object Rows {
	def apply(cv:ColumnNameValue):Rows = {
		var r = new Rows()
		r.rows.append(cv)
		r
	}
}

case class ColumnFamily(val ks:Keyspace, val name:String) extends Cassandra with LogHelper {
	import me.prettyprint.hector.api.factory.HFactory
	import me.prettyprint.hector.api.ddl.ComparatorType
	import me.prettyprint.cassandra.service.ThriftKsDef
	import me.prettyprint.hector.api.exceptions.HInvalidRequestException
	import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
	import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
	import java.util.Arrays
	import Conversions._
	
	var cfDef:ColumnFamilyDefinition = null
	var newKeyspace:KeyspaceDefinition = null
		
	def ->(row: String) = new Row(this,row)	
	
	//get data out of this column family
	def >> (sets: (MultigetSliceQuery[String,String,String]) => Unit,  proc: (String, String, String) => Unit) {
		Cassandra >> (this, sets, proc)
	}	
	
	//get data of this counter column family
	def ># (sets: (MultigetSliceCounterQuery[String,String]) => Unit,  proc: (String, String, Long) => Unit) = {
		Cassandra ># (this, sets, proc)
	}	
	
	//get data of this counter column family
	def >% (sets: (CounterQuery[String,String]) => Unit,  proc: (Long) => Unit) = {
		Cassandra >% (this, sets, proc)
	}	
}

case class Keyspace(val name: String) {

	val replicationFactor = 1
	def create = Unit
	def \(cf:String) = new ColumnFamily(this, cf)
}