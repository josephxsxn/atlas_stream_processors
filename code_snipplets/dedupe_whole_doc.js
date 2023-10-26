        w = {
                $tumblingWindow: {
                    interval: {size: NumberInt(5), unit: "second"},
                    pipeline: [
                        { $group : {
                            _id : '$$ROOT'
                        },
                    },
                    {$replaceRoot: {newRoot: '$_id'}},
                    {$group : {
                        _id : '$entity',
                         'sumofcount' : {$sum : '$count'}
                    }
                    }  
                    ]
                }
            }
