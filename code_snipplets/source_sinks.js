--Full Document Change Stream from a Collection
s = {
        $source:  {
            connectionName: 'jsncluster0',
            db: 'asp_poc_db',
            coll: 'sensors_data',
            fullDocument: 'required'
        }
    }

--Full Document Change Stream from a Database (and all collections within)
s = {
        $source:  {
            connectionName: 'jsncluster0',
            db: 'asp_poc_db',
            fullDocument: 'required'
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
