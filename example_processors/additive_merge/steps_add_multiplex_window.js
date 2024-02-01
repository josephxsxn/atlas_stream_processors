db.createCollection("steps_daily")
db.steps_daily.createIndex({"report_id": 1}, {unique: true})
db.createCollection("steps_weekly")
db.steps_weekly.createIndex({"report_id": 1}, {unique: true})
db.createCollection("steps_monthly")
db.steps_monthly.createIndex({"report_id": 1}, {unique: true})
db.createCollection("steps_yearly")
db.steps_yearly.createIndex({"report_id": 1}, {unique: true})

db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate("2022-12-01T14:10:30Z")}) //2022
db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate("2023-05-01T14:10:30Z")}) //2023 05
db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate("2023-12-01T14:10:30Z")}) // 2023 12
db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate("2023-12-29T14:10:30Z")}) //2023 12 52
db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate("2023-12-30T14:10:30Z")}) // 2023 12 52
db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate("2023-12-30T14:11:30Z")}) // 2023 12 52
db.hextest.insertOne({hex:"03C8", user : "joe", timstam : new ISODate()}) //now


s = {
    $source:  {
        connectionName: 'jsncluster0',
        db: 'test',
        coll: 'hextest',
        timeField :  { $dateFromString : { "dateString" : {$dateToString: { date : "$fullDocument.timstam"}}}},
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
       "daily", "weekly", "monthly", "yearly"
    ],
    steps_day : {$toString: {$dayOfMonth : "$timstam"}},
    steps_week : {$toString: {$week : "$timstam"}},
    steps_month : { $toString:{$month : "$timstam"}},
    steps_year : { $toString:{$year : "$timstam"}},
    strtime : {$dateToString: { date : "$timstam"}}
}}

uw = { '$unwind': '$reports' }

idgen = {$addFields : { report_id : 
      {$switch : {
        branches: [
          { case: { $eq: ["$reports", "weekly"]}, then: {$concat : ["$user", "-", "$steps_week", "$steps_year"] } },
          { case: { $eq: ["$reports", "monthly"]}, then: {$concat : ["$user", "-", "$steps_month", "-", "$steps_year"] } },
          { case: { $eq: ["$reports", "yearly"]}, then: {$concat : ["$user", "-", "$steps_year"] } },

        ],
        default: {$concat : ["$user", "-", "$steps_day", "-", "$steps_month", "-", "$steps_year"]}}
      } }}

unset = {$unset : ["_id", "strtime", "timstam"] }

w = {
  $tumblingWindow: {
      interval: {size: NumberInt(60), unit: "minute"},
      idleTimeout: {size: NumberInt(5), unit: "minute"},
      allowedLateness : {size: NumberInt(15), unit: "minute"},
      pipeline: [
          { $group : {
              _id : { report_id: '$report_id', reports : '$reports', user : '$user'},
              steps : {$sum : '$steps'}
          },
      },
      ]
  }
}

p2 = {$project : { 
  _id : 0,
  report_id : '$_id.report_id',
  user : '$_id.user',
  reports : '$_id.reports',
  steps : 1
}}


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

sp.createStreamProcessor('hex', [s,rr,af,p,uw,idgen,unset,w,p2,m], dlq)

sp.process([s,rr,af,p,uw,idgen,unset,w,p2,m])


db.steps_daily.remove({})
db.steps_weekly.remove({})
db.steps_yearly.remove({})
db.steps_monthly.remove({})

db.steps_daily.find()
db.steps_weekly.find()
db.steps_yearly.find()
db.steps_monthly.find()
