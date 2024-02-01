db.hextest.insertOne({hex:"03C8"})


p = {$project: {hexDec : { 
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


sp.process([s,rr,p])

/*
  {
  _id: ObjectId("65bbacf71885149d4b1f0308"),
  hexDec: 968,
  _stream_meta: {
    sourceType: 'atlas',
    timestamp: ISODate("2024-02-01T14:38:47.661Z")
  }
}
  */
