s = {$source : {
    connectionName : "kafkaprod",
    topic : ["temperature", "humidity"],
    timeField : { $dateFromString: { dateString: '$timestamp' } },
    partitionIdleTimeout: { "size": 5,"unit": "second" }
}}

w = {$tumblingWindow : {
        interval: {size: 30, unit: "second"},
        idleTimeout: {size: 10, unit: "second"},
        allowedLateness : {size: 5, unit: "second"},
        pipeline : [
            { $group : {
                _id : "$sensorIdGroup",      
                humidity: {$top : { output : ["$humidity", "$_ts"],  sortBy : { "timestamp" : -1, "humidity" : -1,  }}},
                temperature: {$top : { output : ["$temperature", "$_ts"],  sortBy : { "timestamp" : -1, "temperature" : -1,  }}},
            }},
            ]
}}
p = {$project : {
    _id :1,
    humidity : {$arrayElemAt : ["$humidity" ,0]},
    temperature :  {$arrayElemAt : ["$temperature" ,0]},
}}
m = {
    $merge: {
        into: {
            connectionName: 'jsncluster0',
            db: 'sensorData',
            coll: 'weather'
        },
        on: ['_id'],
        whenMatched: 'merge',
        whenNotMatched: 'insert'
    }
}

dlq = {dlq: {connectionName: "jsncluster0", db: "sensorData", coll: "sensorDLQ"}}
sp.createStreamProcessor('sensorJoins',[s,w,p,m],dlq)
sp.sensorJoins.start()
