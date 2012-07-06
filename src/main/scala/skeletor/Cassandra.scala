package github.joestein.skeletor

import me.prettyprint.hector.api.{ Cluster, Keyspace => HKeyspace }
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.query.{ MultigetSliceQuery, MultigetSliceCounterQuery, CounterQuery, RangeSlicesQuery }
import github.joestein.util.{ LogHelper }
import Conversions._
import me.prettyprint.hector.api.query.Query
import me.prettyprint.hector.api.beans.{ Rows => HectorRows }
import me.prettyprint.cassandra.model.IndexedSlicesQuery
import me.prettyprint.hector.api.Serializer

object Cassandra extends LogHelper {
    //https://github.com/rantav/hector/blob/master/core/src/main/java/me/prettyprint/hector/api/factory/HFactory.java

    var cluster: Cluster = null

    def *(name: String, servers: String) = {
        cluster = HFactory.getOrCreateCluster(name, servers);
    }

    def connect(name: String, servers: String) = {
        *(name, servers)
    }

    def shutdown() = {
        cluster.getConnectionManager().shutdown()

    }
    import scala.collection.mutable.ListBuffer
    import me.prettyprint.cassandra.serializers.LongSerializer
    import me.prettyprint.cassandra.serializers.StringSerializer
    import me.prettyprint.hector.api.ConsistencyLevelPolicy

    //default write consistency
    var defaultWriteConsistencyLevel: ConsistencyLevelPolicy = {
        CL.ONE()
    }

    //default read consistency
    var defaultReadConsistencyLevel: ConsistencyLevelPolicy = {
        CL.ONE()
    }

    def ++(rows: Seq[ColumnNameValue], cl: ConsistencyLevelPolicy = defaultWriteConsistencyLevel): Unit = {
        val stringSerializer = StringSerializer.get()
        val ksp = HFactory.createKeyspace(rows(0).ks, cluster);
        ksp.setConsistencyLevelPolicy(cl) //this way you can set your own consistency level

        val mutator = HFactory.createMutator(ksp, stringSerializer);

        rows.foreach { cv =>
            mutator.insertCounter(cv.row, cv.cf, HFactory.createCounterColumn(cv.name, cv.intValue))
        }

        mutator.execute()
    }

    def <<(rows: Seq[ColumnNameValue], cl: ConsistencyLevelPolicy = defaultWriteConsistencyLevel): Unit = {

        if (rows(0).isCounter) { //it is a counter column to shoot it on up
            ++(rows, cl) //this way you can set your own consistency level
        } else {
            val stringSerializer = StringSerializer.get()
            val ksp = HFactory.createKeyspace(rows(0).ks, cluster);
            ksp.setConsistencyLevelPolicy(cl) //this way you can set your own consistency level

            val mutator = HFactory.createMutator(ksp, stringSerializer);

            rows.foreach { cv =>
                mutator.addInsertion(cv.row, cv.cf, cv.hColumn)
            }

            mutator.execute()
        }
    }

    def indexQuery[V](cf: ColumnFamily, settings: (IndexedSlicesQuery[String, String, V]) => Unit, proc: (String, String, V) => Unit, valueSerializer: Serializer[V], cl: ConsistencyLevelPolicy = defaultReadConsistencyLevel) = {
        val stringSerializer = StringSerializer.get()
        val ksp = HFactory.createKeyspace(cf.ks, cluster);
        ksp.setConsistencyLevelPolicy(cl) //this way you can set your own consistency level

        val query = HFactory.createIndexedSlicesQuery(ksp, stringSerializer, stringSerializer, valueSerializer)
        query.setColumnFamily(cf);

        settings(query); //let the caller define keys, range, count whatever they want on this CF

        executeQuery(query, proc)
    }

    def rangeQuery(cf: ColumnFamily, settings: (MultigetSliceQuery[String, String, String]) => Unit, proc: (String, String, String) => Unit, cl: ConsistencyLevelPolicy = defaultReadConsistencyLevel) = {
        val stringSerializer = StringSerializer.get()
        val ksp = HFactory.createKeyspace(cf.ks, cluster);
        ksp.setConsistencyLevelPolicy(cl) //this way you can set your own consistency level

        val multigetSliceQuery = HFactory.createMultigetSliceQuery(ksp, stringSerializer, stringSerializer, stringSerializer)
        multigetSliceQuery.setColumnFamily(cf);

        settings(multigetSliceQuery); //let the caller define keys, range, count whatever they want on this CF

        executeQuery(multigetSliceQuery, proc)
    }

    def >>(cf: ColumnFamily, settings: (MultigetSliceQuery[String, String, String]) => Unit, proc: (String, String, String) => Unit, cl: ConsistencyLevelPolicy = defaultReadConsistencyLevel) = {
        rangeQuery(cf, settings, proc, cl)
    }

    def slicesQuery(cf: ColumnFamily, settings: (RangeSlicesQuery[String, String, String]) => Unit, proc: (String, String, String) => Unit, cl: ConsistencyLevelPolicy = defaultReadConsistencyLevel) = {
        val stringSerializer = StringSerializer.get()
        val ksp = HFactory.createKeyspace(cf.ks, cluster)
        ksp.setConsistencyLevelPolicy(cl)

        val rangeSlicesQuery = HFactory.createRangeSlicesQuery(ksp, stringSerializer, stringSerializer, stringSerializer)
        rangeSlicesQuery.setColumnFamily(cf)

        settings(rangeSlicesQuery)

        executeQuery(rangeSlicesQuery, proc)
    }

    def >>>(cf: ColumnFamily, settings: (RangeSlicesQuery[String, String, String]) => Unit, proc: (String, String, String) => Unit, cl: ConsistencyLevelPolicy = defaultReadConsistencyLevel) = {
        slicesQuery(cf, settings, proc, cl)
    }

    private def executeQuery[V](query: Query[_ <: HectorRows[String, String, V]], proc: (String, String, V) => Unit) = {
        val result = query.execute();
        val orderedRows = result.get();
        import scala.collection.JavaConversions._
        for (o <- orderedRows) {

            val c = o.getColumnSlice()
            val d = c.getColumns()

            for (l <- d) {
                debug("query=" + o.getKey() + " for column=" + l.getName() + " & value=" + l.getValue())
                proc(o.getKey(), l.getName(), l.getValue())
            }
        }
    }

    //delete a row
    def delete(cnv: ColumnNameValue, cl: ConsistencyLevelPolicy = defaultWriteConsistencyLevel) = {
        val stringSerializer = StringSerializer.get()
        val ksp = HFactory.createKeyspace(cnv.ks, cluster);
        ksp.setConsistencyLevelPolicy(cl) //this way you can set your own consistency level

        val mutator = HFactory.createMutator(ksp, stringSerializer);

        if (cnv.name == "")
            mutator.delete(cnv.row, cnv.cf, null, stringSerializer); //setting null for column gets rid of entire row
        else
            mutator.delete(cnv.row, cnv.cf, cnv.name, stringSerializer); //setting null for column gets rid of entire row
    }

    def >#(cf: ColumnFamily, sets: (MultigetSliceCounterQuery[String, String]) => Unit, proc: (String, String, Long) => Unit, cl: ConsistencyLevelPolicy = defaultReadConsistencyLevel) = {
        val stringSerializer = StringSerializer.get()
        val ksp = HFactory.createKeyspace(cf.ks, cluster);
        ksp.setConsistencyLevelPolicy(cl) //this way you can set your own consistency level

        val multigetCounterSliceQuery = HFactory.createMultigetSliceCounterQuery(ksp, stringSerializer, stringSerializer)
        multigetCounterSliceQuery.setColumnFamily(cf);

        sets(multigetCounterSliceQuery); //let the caller define keys, range, count whatever they want on this CF

        val result = multigetCounterSliceQuery.execute();
        val orderedRows = result.get();
        debug("keyMultigetSliceCounterQuery order rows called")
        import scala.collection.JavaConversions._

        for (o <- orderedRows) {

            val c = o.getColumnSlice()
            val d = c.getColumns()

            for (l <- d) {
                debug("keyMultigetSliceCounterQuery=" + o.getKey() + " for column=" + l.getName() + " & value=" + l.getValue())
                proc(o.getKey(), l.getName(), l.getValue())
            }
        }
    }

    def >%(cf: ColumnFamily, sets: (CounterQuery[String, String]) => Unit, proc: (Long) => Unit, cl: ConsistencyLevelPolicy = defaultReadConsistencyLevel) = {
        val stringSerializer = StringSerializer.get()
        val ksp = HFactory.createKeyspace(cf.ks, cluster);
        ksp.setConsistencyLevelPolicy(cl) //this way you can set your own consistency level

        val getCounterQuery = HFactory.createCounterColumnQuery(ksp, stringSerializer, stringSerializer)
        getCounterQuery.setColumnFamily(cf)

        sets(getCounterQuery); //let the caller define keys, range, count whatever they want on this CF

        val result = getCounterQuery.execute();
        val counter = result.get();

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