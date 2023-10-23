w2 =  { $tumblingWindow: {
                interval: {size: NumberInt(30), unit: "second"},
                        "pipeline": [ 
                            {
                                $group: {
                                    "_id" : { "device_id" : "$device_id"},                             
                                },
                            }, {
                                $group: {
                                    "_id" : 1,
                                    count: {$sum : 1}
                                }
                            },
                           

                        ],
                     } 
                 }
