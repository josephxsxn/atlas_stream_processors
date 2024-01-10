--Full Document Change Stream from a Collection
s = {
        $source:  {
            connectionName: 'jsncluster0',
            db: 'asp_poc_db',
            coll: 'sensors_data',
            config : {
                  fullDocument: 'whenAvailable',
                },        
        }
    }

--Full Document Change Stream from a Database (and all collections within)
s = {
        $source:  {
            connectionName: 'jsncluster0',
            db: 'asp_poc_db',
            config : {
                  fullDocument: 'whenAvailable',
                },        
        }
    }

--Merge to a Atlas Cluster Collection
merge = {
    $merge: {
        into: {
            connectionName: 'jsncluster0',
            db: 'asp_poc_db',
            coll: 'silver1'
        },
        on: ['_id'],
        whenMatched: 'merge',
        whenNotMatched: 'insert'
    }
}

--Dynamic Merge
    merge =  {$merge: {
        into: {
            connectionName: "jsncluster0",
            db: "test",
            coll: { $cond: { if: {$expr : {$lte : ['$count', '$filter_rule']}}, then: "countData", else: { $concat: ["countData_",{$toString: '$count'}]} } }},
            on: ["entity"],
            whenMatched: "replace"

    }}

--Emit to a Kafka Topic
e =     { $emit: { 
            connectionName: 'aaeh', 
             topic: 'sometopic' 
         }}


--Kafka Start At Latest/Earliest
s1={
  '$source': {
    connectionName: 'ccloud',
    topic: 'Stocks',
    timeField: { '$dateFromString': { dateString: '$tx_time' } },
    config: { startAt: 'earliest' }
  }
}
