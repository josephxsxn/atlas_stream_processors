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

dlq = {dlq: {connectionName: "jsncluster0", db: "test", coll: "steps_dlq"}}

sp.createStreamProcessor('steps', [s,rr,af,p,uw,idgen,unset,m], dlq)

sp.process([s,rr,af,p,uw,idgen,unset,m])


db.steps_raw.remove({})
db.steps_weekly.remove({})
db.steps_yearly.remove({})
db.steps_monthly.remove({})


/*
    Atlas atlas-ec9c8m-shard-0 [primary] test> db.steps_raw.find()
[
  {
    _id: ObjectId("65bbc1fc06169f8e7c8e1bc2"),
    user: 'joe',
    timstam: ISODate("2024-02-01T16:08:26.575Z"),
    steps: 968,
    reports: 'raw',
    steps_week: '4',
    steps_month: '2',
    steps_year: '2024',
    report_id: 'joe-2024-02-01T16:08:26.575Z',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:26.645Z")
    }
  },
  {
    _id: ObjectId("65bbc20006169f8e7c8e1c24"),
    user: 'joe',
    timstam: ISODate("2022-12-01T14:10:30.000Z"),
    steps: 968,
    reports: 'raw',
    steps_week: '48',
    steps_month: '12',
    steps_year: '2022',
    report_id: 'joe-2022-12-01T14:10:30.000Z',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:30.476Z")
    }
  },
  {
    _id: ObjectId("65bbc20406169f8e7c8e1c85"),
    user: 'joe',
    timstam: ISODate("2023-05-01T14:10:30.000Z"),
    steps: 968,
    reports: 'raw',
    steps_week: '18',
    steps_month: '5',
    steps_year: '2023',
    report_id: 'joe-2023-05-01T14:10:30.000Z',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:34.292Z")
    }
  },
  {
    _id: ObjectId("65bbc20806169f8e7c8e1cf8"),
    user: 'joe',
    timstam: ISODate("2023-12-01T14:10:30.000Z"),
    steps: 968,
    reports: 'raw',
    steps_week: '48',
    steps_month: '12',
    steps_year: '2023',
    report_id: 'joe-2023-12-01T14:10:30.000Z',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:38.093Z")
    }
  },
  {
    _id: ObjectId("65bbc20c06169f8e7c8e1d5b"),
    user: 'joe',
    timstam: ISODate("2023-12-30T14:10:30.000Z"),
    steps: 968,
    reports: 'raw',
    steps_week: '52',
    steps_month: '12',
    steps_year: '2023',
    report_id: 'joe-2023-12-30T14:10:30.000Z',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:41.737Z")
    }
  },
  {
    _id: ObjectId("65bbc20e06169f8e7c8e1da4"),
    user: 'joe',
    timstam: ISODate("2023-12-29T14:10:30.000Z"),
    steps: 968,
    reports: 'raw',
    steps_week: '52',
    steps_month: '12',
    steps_year: '2023',
    report_id: 'joe-2023-12-29T14:10:30.000Z',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:45.043Z")
    }
  }
]
Atlas atlas-ec9c8m-shard-0 [primary] test> db.steps_weekly.find()
[
  {
    _id: ObjectId("65bbc1fc06169f8e7c8e1bac"),
    user: 'joe',
    timstam: ISODate("2024-02-01T16:08:26.575Z"),
    steps: 968,
    reports: 'weekly',
    steps_week: '4',
    steps_month: '2',
    steps_year: '2024',
    report_id: 'joe-4-2-2024',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:26.645Z")
    }
  },
  {
    _id: ObjectId("65bbc20006169f8e7c8e1c10"),
    user: 'joe',
    timstam: ISODate("2022-12-01T14:10:30.000Z"),
    steps: 968,
    reports: 'weekly',
    steps_week: '48',
    steps_month: '12',
    steps_year: '2022',
    report_id: 'joe-48-12-2022',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:30.476Z")
    }
  },
  {
    _id: ObjectId("65bbc20406169f8e7c8e1c7b"),
    user: 'joe',
    timstam: ISODate("2023-05-01T14:10:30.000Z"),
    steps: 968,
    reports: 'weekly',
    steps_week: '18',
    steps_month: '5',
    steps_year: '2023',
    report_id: 'joe-18-5-2023',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:34.292Z")
    }
  },
  {
    _id: ObjectId("65bbc20806169f8e7c8e1cee"),
    user: 'joe',
    timstam: ISODate("2023-12-01T14:10:30.000Z"),
    steps: 968,
    reports: 'weekly',
    steps_week: '48',
    steps_month: '12',
    steps_year: '2023',
    report_id: 'joe-48-12-2023',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:38.093Z")
    }
  },
  {
    _id: ObjectId("65bbc20c06169f8e7c8e1d51"),
    user: 'joe',
    timstam: ISODate("2023-12-30T14:10:30.000Z"),
    steps: 1936,
    reports: 'weekly',
    steps_week: '52',
    steps_month: '12',
    steps_year: '2023',
    report_id: 'joe-52-12-2023',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:41.737Z")
    }
  }
]
Atlas atlas-ec9c8m-shard-0 [primary] test> db.steps_monthly.find()
[
  {
    _id: ObjectId("65bbc1fc06169f8e7c8e1bb7"),
    user: 'joe',
    timstam: ISODate("2024-02-01T16:08:26.575Z"),
    steps: 968,
    reports: 'monthly',
    steps_week: '4',
    steps_month: '2',
    steps_year: '2024',
    report_id: 'joe-2-2024',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:26.645Z")
    }
  },
  {
    _id: ObjectId("65bbc20006169f8e7c8e1c1a"),
    user: 'joe',
    timstam: ISODate("2022-12-01T14:10:30.000Z"),
    steps: 968,
    reports: 'monthly',
    steps_week: '48',
    steps_month: '12',
    steps_year: '2022',
    report_id: 'joe-12-2022',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:30.476Z")
    }
  },
  {
    _id: ObjectId("65bbc20406169f8e7c8e1c8f"),
    user: 'joe',
    timstam: ISODate("2023-05-01T14:10:30.000Z"),
    steps: 968,
    reports: 'monthly',
    steps_week: '18',
    steps_month: '5',
    steps_year: '2023',
    report_id: 'joe-5-2023',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:34.292Z")
    }
  },
  {
    _id: ObjectId("65bbc20806169f8e7c8e1d02"),
    user: 'joe',
    timstam: ISODate("2023-12-01T14:10:30.000Z"),
    steps: 2904,
    reports: 'monthly',
    steps_week: '48',
    steps_month: '12',
    steps_year: '2023',
    report_id: 'joe-12-2023',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:38.093Z")
    }
  }
]
Atlas atlas-ec9c8m-shard-0 [primary] test> db.steps_yearly.find()
[
  {
    _id: ObjectId("65bbc1fc06169f8e7c8e1bcd"),
    user: 'joe',
    timstam: ISODate("2024-02-01T16:08:26.575Z"),
    steps: 968,
    reports: 'yearly',
    steps_week: '4',
    steps_month: '2',
    steps_year: '2024',
    report_id: 'joe-2024',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:26.645Z")
    }
  },
  {
    _id: ObjectId("65bbc20006169f8e7c8e1c2d"),
    user: 'joe',
    timstam: ISODate("2022-12-01T14:10:30.000Z"),
    steps: 968,
    reports: 'yearly',
    steps_week: '48',
    steps_month: '12',
    steps_year: '2022',
    report_id: 'joe-2022',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:30.476Z")
    }
  },
  {
    _id: ObjectId("65bbc20406169f8e7c8e1c99"),
    user: 'joe',
    timstam: ISODate("2023-05-01T14:10:30.000Z"),
    steps: 3872,
    reports: 'yearly',
    steps_week: '18',
    steps_month: '5',
    steps_year: '2023',
    report_id: 'joe-2023',
    _stream_meta: {
      sourceType: 'atlas',
      timestamp: ISODate("2024-02-01T16:08:34.292Z")
    }
  }
]
    */
