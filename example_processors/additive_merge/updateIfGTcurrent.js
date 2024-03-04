//BASED ON _TS, update on _id
db.createCollection("insertIngest")
db.insertIngest.insertOne({count : 10, data : "old", id :1})
db.insertIngest.insertOne({count : 20, data : "old", id :2})
db.insertIngest.update({id : 1},{$set : { count : 11, data : "new"}})
db.insertIngest.update({id : 2},{$set : { count : 22, data : "new"}})

db.createCollection("insertTest")
db.insertTest.createIndex({"id": 1}, {unique: true})

s = {$source : {
    connectionName : "jsncluster0",
    db : "test",
    coll : "insertIngest",
    config : {fullDocument : "whenAvailable"}
}}

 rr = {
  $replaceRoot: { newRoot :  { $mergeObjects: [ "$fullDocument", { "_ts" : "$_ts"} ] }}
}

    m =  {$merge: {
        into: {
            connectionName: "jsncluster0",
            db: "test",
            coll: "insertTest"},
            let : {ingestTime : "$_ts", orig : "$$ROOT"},
            on: ["_id"],
             whenMatched: [
                            { $project: { 
                                id : 1,
                                out:   { $cond: { if: { $gt: ["$$ingestTime", "$_ts"], }, then: "$$orig", else: "$$ROOT"  }}
                            }},
                            { $replaceRoot:  { newRoot: { $mergeObjects : ["$out", {id : "$id"}] }}},
                            {$unset : ["_id"]}
                        ]
    }}

    //output
    Atlas atlas-ec9c8m-shard-0 [primary] test> db.insertTest.find({})
    [
      {
        _id: ObjectId("65e5d5cbd7d335144cc569ef"),
        count: 11,
        data: 'new',
        id: 1,
        _ts: ISODate("2024-03-04T14:09:05.238Z"),
        _stream_meta: {
          sourceType: 'atlas',
          timestamp: ISODate("2024-03-04T14:09:05.238Z")
        }
      },
      {
        _id: ObjectId("65e5d5cbd7d335144cc569f0"),
        count: 22,
        data: 'new',
        id: 2,
        _ts: ISODate("2024-03-04T14:09:15.680Z"),
        _stream_meta: {
          sourceType: 'atlas',
          timestamp: ISODate("2024-03-04T14:09:15.680Z")
        }
      }
    ]

//BASED ON INT FIELD
db.insertIngest.insertOne({count : 10, data : "old", id :1})
db.insertIngest.insertOne({count : 10, data : "new", id :2})

db.insertTest.createIndex({"id": 1}, {unique: true})
db.insertTest.insertMany([{count : 11, data : "abc", id : 1}, {count : 9, data : "efg", id : 2}])

s = {$source : {
    connectionName : "jsncluster0",
    db : "test",
    coll : "insertIngest",
    config : {fullDocument : "whenAvailable"}
}}

rr = {
    $replaceRoot: { newRoot : "$fullDocument"}
 }


    m =  {$merge: {
        into: {
            connectionName: "jsncluster0",
            db: "test",
            coll: "insertTest"},
            let : {ingestCount : "$count", orig : "$$ROOT"},
            on: ["id"],
             whenMatched: [
                            { $project: { 
                                id : 1,
                                out:   { $cond: { if: { $gt: ["$$ingestCount", "$count"], }, then: "$$orig", else: "$$ROOT"  }}
                            }},
                            { $replaceRoot:  { newRoot: { $mergeObjects : ["$out", {id : "$id"}] }}},
                            {$unset : ["_id"]}
                        ]
    }}

    //output
    Atlas atlas-ec9c8m-shard-0 [primary] test> db.insertTest.find({})
    [
      {
        _id: ObjectId("65d7bfb0d7d335144cc569e2"),
        count: 11,
        data: 'abc',
        id: 1
      },
      {
        _id: ObjectId("65d7bfb0d7d335144cc569e3"),
        count: 10,
        data: 'new',
        id: 2,
        _stream_meta: {
          sourceType: 'atlas',
          timestamp: ISODate("2024-02-22T21:42:53.802Z")
        }
      }
    ]
