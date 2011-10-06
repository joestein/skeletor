package github.joestein.skeletor

// more info on consitency settings = http://www.datastax.com/docs/0.8/dml/data_consistency


import me.prettyprint.cassandra.service.{OperationType}
import me.prettyprint.cassandra.model.{AllOneConsistencyLevelPolicy, QuorumAllConsistencyLevelPolicy}
import me.prettyprint.hector.api.{ConsistencyLevelPolicy, HConsistencyLevel}

object CL {

	/*A write must be written to at least one node. If all replica nodes for the given row key are down, 
	the write can still succeed once a hinted handoff has been written. Note that if all replica nodes are 
	down at write time, an ANY write will not be readable until the replica nodes for that row key have recovered.*/
	object ANY {
		def apply() = {
			new AnyConsistencyLevelPolicy()
		}
	}
	
	/*
	A write must be written to the commit log and memory table on all replica nodes in the cluster for that row key.
	Returns the record with the most recent timestamp once all replicas have responded. 
	The read operation will fail if a replica does not respond. */
	object ALL {
		def apply() = {
			new AllConsistencyLevelPolicy()
		}
	}
		
	/*A write must be written to the commit log and memory table of at least one replica node.
	Returns a response from the closest replica (as determined by the snitch). 
	By default, a read repair runs in the background to make the other replicas consistent.*/
	object ONE {
		def apply() = {
			new AllOneConsistencyLevelPolicy()
		}
	}
			
	/*A write must be written to the commit log and memory table on a quorum of replica nodes.
	Returns the record with the most recent timestamp once a quorum of replicas has responded.*/
	object QUARUM {
		def apply() = {
			new QuorumAllConsistencyLevelPolicy()
		}
	}
	
	/*A write must be written to the commit log and memory table on a quorum of replica nodes in the same data 
	center as the coordinator node. Avoids latency of inter-data center communication.
	Returns the record with the most recent timestamp once a quorum of replicas in the current data center as the coordinator 
	node has reported. Avoids latency of inter-data center communication.*/
	object LOCALQUARUM {
		def apply() = {
			new LocalQuorumConsistencyLevelPolicy()
		}
	}
	
	/*A write must be written to the commit log and memory table on a quorum of replica nodes in all data centers.
	Returns the record with the most recent timestamp once a quorum of replicas in each data center of the cluster has responded.*/
	object EACHQUARUM {
		def apply() = {
			new EachQuorumConsistencyLevelPolicy()
		}
	}		
}

class AnyConsistencyLevelPolicy extends ConsistencyLevelPolicy {
	def get(op: OperationType): HConsistencyLevel = {
	    return HConsistencyLevel.ANY;
	}	
	
	def get(op: OperationType, cfName: String): HConsistencyLevel = {
	    return HConsistencyLevel.ANY;
	}
}

class AllConsistencyLevelPolicy extends ConsistencyLevelPolicy {
	def get(op: OperationType): HConsistencyLevel = {
	    return HConsistencyLevel.ALL;
	}	
	
	def get(op: OperationType, cfName: String): HConsistencyLevel = {
	    return HConsistencyLevel.ALL;
	}
}

class EachQuorumConsistencyLevelPolicy extends ConsistencyLevelPolicy {
	def get(op: OperationType): HConsistencyLevel = {
	    return HConsistencyLevel.EACH_QUORUM;
	}	
	
	def get(op: OperationType, cfName: String): HConsistencyLevel = {
	    return HConsistencyLevel.EACH_QUORUM;
	}
}

class LocalQuorumConsistencyLevelPolicy extends ConsistencyLevelPolicy {
	def get(op: OperationType): HConsistencyLevel = {
	    return HConsistencyLevel.LOCAL_QUORUM;
	}	
	
	def get(op: OperationType, cfName: String): HConsistencyLevel = {
	    return HConsistencyLevel.LOCAL_QUORUM;
	}
}





