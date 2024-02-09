db.gpsCords.insertOne({ "vehicle_id": "vid0001", "coord": [37.76643495, -122.3969431 ] })
db.gpsCords.insertOne({ "vehicle_id": "vid0001", "coord": [38.76643495, -123.3969431 ] })
db.gpsCords.insertOne({ "vehicle_id": "vid0001", "coord": [39.76643495, -124.3969431 ] })
db.gpsCords.insertOne({ "vehicle_id": "vid0001", "coord": [40.76643495, -125.3969431 ] })

w = { 
    $tumblingWindow: {
      interval: {size: NumberInt(10), unit: "second"},
      idleTimeout : {size: NumberInt(5), unit: "second"},
      "pipeline": 
      [ 
        {
          '$group': {
            '_id': '$vehicle_id', 
            'positions': {
              '$push': {
                'coord':"$coord" 
              }
            }
          }
        }
      ]
    }
  }


db.gpsCord_res.find()
[
  {
    _id: 'vid0001',
    _stream_meta: {
      sourceType: 'atlas',
      windowStartTimestamp: ISODate("2024-02-09T18:23:00.000Z"),
      windowEndTimestamp: ISODate("2024-02-09T18:23:10.000Z")
    },
    positions: [
      { coord: [ 37.76643495, -122.3969431 ] },
      { coord: [ 38.76643495, -123.3969431 ] },
      { coord: [ 39.76643495, -124.3969431 ] },
      { coord: [ 40.76643495, -125.3969431 ] }
    ]
  }
]
