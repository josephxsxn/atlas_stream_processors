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
