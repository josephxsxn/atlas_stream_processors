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

--Change Stream Source starting in the past of the oplog with startAtOperationTime        
s = {
    $source:  {
        connectionName: 'jsncluster0',
        db: 'test',
        coll : 'datetest',
        config : {
              startAtOperationTime : Timestamp(new Date('2024-01-30T00:00:01Z').getTime()/1000,0),
            },        
    }
}

--Change Stream Source starting with OpLog resume token
s = {
    $source:  {
        connectionName: 'jsncluster0',
        db: 'test',
        coll : 'datetest',
        config : {
            startAfter : new Object({_data : '8265B8F36B000000022B042C0100296E5A100469E03217B2954C6580FCAD5A7D225571463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F6964006465B8F36BBDD90E425EC09B31000004'}),
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
    config: { auto_offset_reset: 'earliest' }
  }
}
