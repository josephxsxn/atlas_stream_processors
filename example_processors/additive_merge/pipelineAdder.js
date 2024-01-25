db.createCollection("bank")
db.bank.createIndex({"name": 1}, {unique: true})

db.createCollection("transactions")

//setup full document change streams on collections
var cols = db.getCollectionNames()

for (const el of cols){
    db.runCommand( {
        collMod: el,
        changeStreamPreAndPostImages: { enabled: true }
    } )
}

//bank account balances
db.bank.insertMany([
{name : "joe",
money : 10.00},
{name : "jane",
money : 50.00}])


//transactions stream, insert after processor is running
db.transactions.insertMany([
{name : "joe",
money : 100},
{name : "jane",
money : -10.50}
])

s = {
    $source:  {
        connectionName: 'jsncluster0',
        db: 'test',
        coll: 'transactions',
        config : {
          fullDocument: 'whenAvailable',
        },    
  }
}
rr =   {
    $replaceRoot: { newRoot : "$fullDocument"}
 }

 m = {$merge : {
  into: {
      connectionName: "jsncluster0",
      db: "test",
      coll: "bank"},
      on: ["name"],
      let : {value : "$money"},
      whenMatched : [ { $addFields: {
        money: { $add:[ "$money", "$$value" ] },
      } }]
}}

sp.process([s,rr,m])

/*
[
  {
    _id: ObjectId("65b2858e26d988e2c11ba3b0"),
    name: 'joe',
    money: 110
  },
  {
    _id: ObjectId("65b2858e26d988e2c11ba3b1"),
    name: 'jane',
    money: 39.5
  }
]*/

