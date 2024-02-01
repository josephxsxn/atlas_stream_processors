db.createCollection("steps_raw")
db.steps_raw.createIndex({"report_id": 1}, {unique: true})
db.createCollection("steps_weekly")
db.steps_weekly.createIndex({"report_id": 1}, {unique: true})
db.createCollection("steps_monthly")
db.steps_monthly.createIndex({"report_id": 1}, {unique: true})
db.createCollection("steps_yearly")
db.steps_yearly.createIndex({"report_id": 1}, {unique: true})

db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate()}) //now
db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate("2022-12-01T14:10:30Z")}) //2022
db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate("2023-05-01T14:10:30Z")}) //2023 05
db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate("2023-12-01T14:10:30Z")}) // 2023 12
db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate("2023-12-30T14:10:30Z")}) // 2023 12 52
db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate("2023-12-29T14:10:30Z")}) //2023 12 53

s = {
    $source:  {
        connectionName: 'jsncluster0',
        db: 'test',
        coll: 'hextest',
        config : {
          fullDocument: 'whenAvailable',
          fullDocumentBeforeChange: 'whenAvailable',
        },    
  }
}

rr = {
  $replaceRoot: {newRoot: '$fullDocument'}
}

af = {$addFields: {hexDec : { 
  $sum: {
    $map: {
       input: { $range: [0, { $strLenBytes: "$hex" }] },
       in: { $multiply: [
             { $pow: [16, { $subtract: [{ $strLenBytes: "$hex" }, { $add: ["$$this", 1] }] }] },
             { $indexOfBytes: ["0123456789ABCDEF", { $toUpper: { $substrBytes: ["$hex", "$$this", 1] } }] }
          ]
       }
    }
 }
}
}}


p = {$project : {
  user : 1,
  steps : "$hexDec",
  timstam : 1,
  reports : [
      "raw", "weekly", "monthly", "yearly"
    ],
    steps_week : {$toString: {$week : "$timstam"}},
    steps_month : { $toString:{$month : "$timstam"}},
    steps_year : { $toString:{$year : "$timstam"}},
    strtime : {$dateToString: { date : "$timstam"}}
}}

uw = { '$unwind': '$reports' }

idgen = {$addFields : { report_id : 
      {$switch : {
        branches: [
          { case: { $eq: ["$reports", "weekly"]}, then: {$concat : ["$user", "-", "$steps_week", "-", "$steps_month", "-", "$steps_year"] } },
          { case: { $eq: ["$reports", "monthly"]}, then: {$concat : ["$user", "-", "$steps_month", "-", "$steps_year"] } },
          { case: { $eq: ["$reports", "yearly"]}, then: {$concat : ["$user", "-", "$steps_year"] } },

        ],
        default: {$concat : ["$user","-","$strtime"]}}
      } }}

unset = {$unset : ["_id", "strtime"] }


m = {$merge : {
  into: {
      connectionName: "jsncluster0",
      db: "test",
      coll: {$concat : ["steps_", "$reports"]}},
      on: ["report_id"],
      let : {value : "$steps"},
      whenMatched : [ { $addFields: {
        steps: { $add:[ "$steps", "$$value" ] },
      } }],
     whenNotMatched: "insert"
}}

dlq = {dlq: {connectionName: "jsncluster0", db: "test", coll: "hex_dlq"}}

sp.createStreamProcessor('hex', [s,rr,af,p,uw,idgen,unset,m], dlq)

sp.process([s,rr,af,p,uw,idgen,unset,m])


db.steps_raw.remove({})
db.steps_weekly.remove({})
db.steps_yearly.remove({})
db.steps_monthly.remove({})
