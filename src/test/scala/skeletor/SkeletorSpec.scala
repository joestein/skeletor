import org.specs._
import github.joestein.skeletor.{Cassandra, Rows}
import java.util.UUID
import me.prettyprint.hector.api.query.{MultigetSliceQuery,MultigetSliceCounterQuery,CounterQuery}
import github.joestein.skeletor.Conversions._
import me.prettyprint.hector.api.{ConsistencyLevelPolicy}
import github.joestein.skeletor.{CL}

class SkeletorSpec extends Specification with Cassandra{
	
	val TestColumnFamily = "FixtureTestSkeletor" \ "TestColumnFamily" //now setup the initial CF
	val CounterTestColumnFamily = "FixtureTestSkeletor" \ "CounterTestColumnFamily" //now setup the initial Counter CF
		
	doBeforeSpec {
		Cassandra connect ("skeletor-spec","localhost:9160")
	}
	
	doAfterSpec {
		Cassandra.cluster.getConnectionManager().shutdown();
	}
	
	//create random and unique row, column and value strings for setting and reading to make sure we are dealing with data for this test run
	def rnv(): (String, String, String) = {
		("row_" + UUID.randomUUID().toString(), "column_" + UUID.randomUUID().toString(), "value_" + UUID.randomUUID().toString())
	}
	
	var defaultReadConsistencyLevel: ConsistencyLevelPolicy = {
		CL.ONE()
	}	
	
	"Skeletor " should  {
		
		"be able to add two rows together into the first" in {
			val cv1 = (TestColumnFamily -> "rowKey1" has "columnName1" of "columnValue1")
			
			var rows1:Rows = Rows(cv1) //add the row to the rows object
			
			(rows1.size == 1) must beTrue
			
			val cv2 = (TestColumnFamily -> "rowKey2" has "columnName2" of "columnValue2")
			
			var rows2:Rows = Rows(cv2) //add the row to the rows object			
			
			rows2.size mustEqual 1
			
			rows1 ++ rows2  //add the second Rows into the first Rows, Rows1 becomes the new rows
			
			rows2.size mustEqual 1 //make sure rows 2 is still 1
			
			rows1.size mustEqual 2 //and rows1 is now equal to 2
		}
		
		"write to Cassandra and read row key" in {
			
			val randRow = rnv()
			val rowKey = randRow._1 //lets take some random unique string to write and verify reading it
			val columnName = randRow._2 //lets take some random unique string to write and verify reading it
			val columnValue = randRow._3 //lets take some random unique string to write and verify reading it
			
			var cv = (TestColumnFamily -> rowKey has columnName of columnValue) //create a column value for a row for this column family

			var rows:Rows = Rows(cv) //add the row to the rows object

			Cassandra.defaultReadConsistencyLevel = defaultReadConsistencyLevel
			//println("push the row=" + rowKey + " into Cassandra, batch mutate counter column=" + columnName + " and value=" + columnValue)
			Cassandra << rows 
			
			def processRow(r:String, c:String, v:String) = {
				(r == rowKey) must beTrue
				(c == columnName) must beTrue
				(v == columnValue) must beTrue
			}
			
			def sets(mgsq: MultigetSliceQuery[String, String, String]) {
				mgsq.setKeys(rowKey) //we want to pull out the row key we just put into Cassandra
				mgsq.setColumnNames(columnName) //and just this column
			}
			
			TestColumnFamily >> (sets, processRow) //get data out of Cassandra and process it
			 
		}
		
		"increment a counter and read the values back with a multi get slice" in {
			val randRow = rnv()
			val rowKey = randRow._1 //lets take some random unique string to write and verify reading it
			val columnName = randRow._2 //lets take some random unique string to write and verify reading it
				
			val col = CounterTestColumnFamily -> rowKey has columnName
							
			var cv = (col inc)
			var rows:Rows = Rows(cv) //add the row to the rows object
			
			Cassandra.defaultReadConsistencyLevel = defaultReadConsistencyLevel
			//println("push the row counter=" + rowKey + " into Cassandra, batch mutate counter column=" + columnName)
			Cassandra << rows //push the row into Cassandra, batch mutate
						
			def processRow(r:String, c:String, v:Long) = {
				//println("processRowCounter="+r+"["+c+"]="+v)
				(r == rowKey) must beTrue
				(c == columnName) must beTrue
				(v == 1) must beTrue
			}
			
			def sets(mgsq: MultigetSliceCounterQuery[String, String]) {
				mgsq.setKeys(rowKey) //we want to pull out the row key we just put into Cassandra
				mgsq.setColumnNames(columnName) //and just this column
			}
			
			CounterTestColumnFamily ># (sets, processRow)  //get data out of Cassandra and process it
					
		}
				
		"increment a counter and read the values back by row and column" in {
			val randRow = rnv()
			val rowKey = randRow._1 //lets take some random unique string to write and verify reading it
			val columnName = randRow._2 //lets take some random unique string to write and verify reading it
				
			val col = CounterTestColumnFamily -> rowKey has columnName
							
			var cv = (col inc)
			var rows:Rows = Rows(cv) //add the row to the rows object
			
			Cassandra.defaultReadConsistencyLevel = defaultReadConsistencyLevel
			//println("push the row counter=" + rowKey + " into Cassandra, batch mutate counter column=" + columnName)
			Cassandra << rows //push the row into Cassandra, batch mutate
						
			def setsGet(cq: CounterQuery[String, String]) {
				cq.setKey(rowKey) //we want to pull out the row key we just put into Cassandra
				cq.setName(columnName) //and just this column
			}
						
			def processGetRow(v:Long) = {
				//println("processRowCounter="+v)
				(v == 1) must beTrue
			}
			
			CounterTestColumnFamily >% (setsGet, processGetRow)  //get data out of Cassandra and process it		
		}		
		
		"increment a counter by more than 1 and read the values back by row and column" in {
			val randRow = rnv()
			val rowKey = randRow._1 //lets take some random unique string to write and verify reading it
			val columnName = randRow._2 //lets take some random unique string to write and verify reading it
				
			val col = CounterTestColumnFamily -> rowKey has columnName
							
			var cv = (col inc 6)
			var rows:Rows = Rows(cv) //add the row to the rows object
			
			Cassandra.defaultReadConsistencyLevel = defaultReadConsistencyLevel
			//println("push the row counter=" + rowKey + " into Cassandra, batch mutate counter column=" + columnName)
			Cassandra << rows //push the row into Cassandra, batch mutate
						
			def setsGet(cq: CounterQuery[String, String]) {
				cq.setKey(rowKey) //we want to pull out the row key we just put into Cassandra
				cq.setName(columnName) //and just this column
			}
						
			def processGetRow(v:Long) = {
				//println("processRowCounter="+v)
				(v == 6) must beTrue
			}
			
			CounterTestColumnFamily >% (setsGet, processGetRow)  //get data out of Cassandra and process it		
		}	
		
		"decrement a counter and read the values back by row and column" in {
			val randRow = rnv()
			val rowKey = randRow._1 //lets take some random unique string to write and verify reading it
			val columnName = randRow._2 //lets take some random unique string to write and verify reading it
				
			val col = CounterTestColumnFamily -> rowKey has columnName
							
			var cv = (col dec)
			var rows:Rows = Rows(cv) //add the row to the rows object
			
			Cassandra.defaultReadConsistencyLevel = defaultReadConsistencyLevel
			//println("push the row counter=" + rowKey + " into Cassandra, batch mutate counter column=" + columnName)
			Cassandra << rows //push the row into Cassandra, batch mutate
						
			def setsGet(cq: CounterQuery[String, String]) {
				cq.setKey(rowKey) //we want to pull out the row key we just put into Cassandra
				cq.setName(columnName) //and just this column
			}
						
			def processGetRow(v:Long) = {
				//println("processRowCounter="+v)
				(v == -1) must beTrue
			}
			
			CounterTestColumnFamily >% (setsGet, processGetRow)  //get data out of Cassandra and process it		
		}	
		
		"decrement a counter by more than 1 and read the values back by row and column" in {
			val randRow = rnv()
			val rowKey = randRow._1 //lets take some random unique string to write and verify reading it
			val columnName = randRow._2 //lets take some random unique string to write and verify reading it
				
			val col = CounterTestColumnFamily -> rowKey has columnName
							
			var cv = (col dec 7)
			var rows:Rows = Rows(cv) //add the row to the rows object
			
			Cassandra.defaultReadConsistencyLevel = defaultReadConsistencyLevel
			//println("push the row counter=" + rowKey + " into Cassandra, batch mutate counter column=" + columnName)
			Cassandra << rows //push the row into Cassandra, batch mutate
						
			def setsGet(cq: CounterQuery[String, String]) {
				cq.setKey(rowKey) //we want to pull out the row key we just put into Cassandra
				cq.setName(columnName) //and just this column
			}
						
			def processGetRow(v:Long) = {
				//println("processRowCounter="+v)
				(v == -7) must beTrue
			}
			
			CounterTestColumnFamily >% (setsGet, processGetRow)  //get data out of Cassandra and process it		
		}			
		
		// more info on consitency settings = http://www.datastax.com/docs/0.8/dml/data_consistency
		"be able to tune consistency" in {
			
			"new default for all reads and another for writes" in {
				
				//new default read consistency
				var defaultReadConsistencyLevel: ConsistencyLevelPolicy = {
					CL.ANY()
				}	
				
				Cassandra.defaultReadConsistencyLevel = defaultReadConsistencyLevel
				
				//new default write consistency
				var defaultWriteConsistencyLevel: ConsistencyLevelPolicy = {
					CL.QUARUM()
				}							
				
				Cassandra.defaultWriteConsistencyLevel = defaultWriteConsistencyLevel
				
				true must beTrue //TODO: some better test than just everything getting to this point without exception
			}
			
			"read and write explicitly with ALL consistency"  in {
				var cv = (TestColumnFamily -> "rowKey" has "columnName" of "columnValue")
				var rows:Rows = Rows(cv) //add the row to the rows object

				var testdefaultReadConsistencyLevel: ConsistencyLevelPolicy = {
					CL.ANY()
				}
				
				Cassandra.defaultReadConsistencyLevel = testdefaultReadConsistencyLevel
				//println("push the row=" + rowKey + " into Cassandra, batch mutate counter column=" + columnName + " and value=" + columnValue)
				Cassandra << (rows, CL.ALL())
				
				true must beTrue //TODO: some better test than just everything getting to this point without exception
			}
		
			"delete an entire row" in {
				val randRow = rnv()
				val rowKey = randRow._1 //lets take some random unique string to write and verify reading it
				val columnName = randRow._2 //lets take some random unique string to write and verify reading it
				val columnValue = randRow._3 //lets take some random unique string to write and verify reading it

				var cv = (TestColumnFamily -> rowKey has columnName of columnValue) //create a column value for a row for this column family

				var rows:Rows = Rows(cv) //add the row to the rows object

				Cassandra.defaultReadConsistencyLevel = defaultReadConsistencyLevel
				//println("push the row=" + rowKey + " into Cassandra, batch mutate counter column=" + columnName + " and value=" + columnValue)
				Cassandra << rows 

				def processRow(r:String, c:String, v:String) = {
					(r == rowKey) must beTrue
					(c == columnName) must beTrue
					(v == columnValue) must beTrue
				}

				def sets(mgsq: MultigetSliceQuery[String, String, String]) {
					mgsq.setKeys(rowKey) //we want to pull out the row key we just put into Cassandra
					mgsq.setColumnNames(columnName) //and just this column
				}

				TestColumnFamily >> (sets, processRow) //get data out of Cassandra and process it			
				
				def deletedRow(r:String, c:String, v:String) = {
					(r != rowKey) must beTrue
					(c != columnName) must beTrue
					(v != columnValue) must beTrue
				}
				
				Cassandra delete rows.rows(0)
				
				TestColumnFamily >> (sets, deletedRow) //get data out of Cassandra and process it							
			}			
		}
	} 
}