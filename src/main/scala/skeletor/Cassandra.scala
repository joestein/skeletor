package github.joestein.skeletor

import me.prettyprint.hector.api.{Cluster, Keyspace=>HKeyspace};
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.query.{MultigetSliceQuery,MultigetSliceCounterQuery,CounterQuery}
import github.joestein.util.{LogHelper}
import Conversions._

object Cassandra extends LogHelper {
	//https://github.com/rantav/hector/blob/master/core/src/main/java/me/prettyprint/hector/api/factory/HFactory.java
	
	var cluster:Cluster = null
	
	def *(name: String, servers: String) = {
		cluster = HFactory.getOrCreateCluster(name,servers);
	}
	
	def connect(name: String, servers: String) = {
		*(name,servers)
	}
	
	import scala.collection.mutable.ListBuffer
	import me.prettyprint.cassandra.serializers.LongSerializer
	import me.prettyprint.cassandra.serializers.StringSerializer
	import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
	
	def ++ (rows:ListBuffer[ColumnNameValue], keyspaceSettings: (HKeyspace) => Unit ): Unit = {
		var stringSerializer = StringSerializer.get()
		val ksp = HFactory.createKeyspace(rows(0).ks, cluster);
		
		keyspaceSettings(ksp) //this way you can set your own consistency level
		
		var mutator = HFactory.createMutator(ksp, stringSerializer);

		rows.foreach { cv =>        
			mutator.insertCounter(cv.row, cv.cf, HFactory.createCounterColumn(cv.name, cv.value.toInt))
		}
		
		mutator.execute()
	}
	
	def defaultKeyspaceSettings(ksp: HKeyspace) = {
		var cp = new AllOneConsistencyLevelPolicy();
		ksp.setConsistencyLevelPolicy(cp)		
	}
		
	def ++ (rows:ListBuffer[ColumnNameValue]): Unit = {
		
		++ (rows,defaultKeyspaceSettings) //use a default consistency of one
	}
	
	def << (rows:ListBuffer[ColumnNameValue], keyspaceSettings: (HKeyspace) => Unit ): Unit = {
	
		if (rows(0).isCounter) {  //it is a counter column to shoot it on up
			++(rows,keyspaceSettings)  //this way you can set your own consistency level
		}
		else {
			var stringSerializer = StringSerializer.get()
			val ksp = HFactory.createKeyspace(rows(0).ks, cluster);
			var mutator = HFactory.createMutator(ksp, stringSerializer);

			keyspaceSettings(ksp)  //this way you can set your own consistency level

			rows.foreach { cv =>        
				mutator.addInsertion(cv.row, cv.cf, HFactory.createStringColumn(cv.name, cv.value))
			}

			mutator.execute()
		}	
	}
	// mutation
	def << (rows:ListBuffer[ColumnNameValue]): Unit = {
		if (rows(0).isCounter) {//it is a counter column to shoot it on up
			++(rows,defaultKeyspaceSettings)  //use a default consistency of one
		}		
		else {
			<< (rows,defaultKeyspaceSettings)  //use a default consistency of one
		}
	}	
	
	def >> (cf: ColumnFamily, sets: (MultigetSliceQuery[String,String,String]) => Unit,  proc: (String, String, String) => Unit) = {
		var stringSerializer = StringSerializer.get()
		val ksp = HFactory.createKeyspace(cf.ks, cluster);
				
		var multigetSliceQuery = HFactory.createMultigetSliceQuery(ksp, stringSerializer, stringSerializer, stringSerializer)
		multigetSliceQuery.setColumnFamily(cf);            

		sets(multigetSliceQuery); //let the caller define keys, range, count whatever they want on this CF

		var result = multigetSliceQuery.execute();
		var orderedRows = result.get();		
		import scala.collection.JavaConversions._
		for (o <- orderedRows) {

			val c = o.getColumnSlice()
			val d = c.getColumns()

			for (l <- d) {
				debug("keyMultigetSliceQuery=" + o.getKey() + " for column=" + l.getName() + " & value=" + l.getValue())
				proc(o.getKey(),l.getName(),l.getValue())
			}
		}		
	}
	
	def ># (cf: ColumnFamily, sets: (MultigetSliceCounterQuery[String,String]) => Unit,  proc: (String, String, Long) => Unit) = {
		var stringSerializer = StringSerializer.get()
		val ksp = HFactory.createKeyspace(cf.ks, cluster);
				
		var multigetCounterSliceQuery = HFactory.createMultigetSliceCounterQuery(ksp, stringSerializer, stringSerializer)
		multigetCounterSliceQuery.setColumnFamily(cf);            

		sets(multigetCounterSliceQuery); //let the caller define keys, range, count whatever they want on this CF

		var result = multigetCounterSliceQuery.execute();
		var orderedRows = result.get();		
		debug("keyMultigetSliceCounterQuery order rows called")
		import scala.collection.JavaConversions._
		
		for (o <- orderedRows) {

			val c = o.getColumnSlice()
			val d = c.getColumns()

			for (l <- d) {
				debug("keyMultigetSliceCounterQuery=" + o.getKey() + " for column=" + l.getName() + " & value=" + l.getValue())
				proc(o.getKey(),l.getName(),l.getValue())
			}
		}
	}	
	
	def >% (cf: ColumnFamily, sets: (CounterQuery[String,String]) => Unit,  proc: (Long) => Unit) = {
		var stringSerializer = StringSerializer.get()
		val ksp = HFactory.createKeyspace(cf.ks, cluster);
				
		var getCounterQuery = HFactory.createCounterColumnQuery(ksp, stringSerializer, stringSerializer)
		getCounterQuery.setColumnFamily(cf)

		sets(getCounterQuery); //let the caller define keys, range, count whatever they want on this CF

		var result = getCounterQuery.execute();
		var counter = result.get();		

		if (counter != null)
			proc(counter.getValue())
	}	
}

trait Cassandra {
	//https://github.com/rantav/hector/blob/master/core/src/main/java/me/prettyprint/cassandra/service/CassandraHostConfigurator.java
	
	def ^ = {
		Cassandra.cluster
	}
}