# atlas_stream_processors
Joe's private repo of Stream Processors


# cmds
sp.process(pipeline)

sp.createStreamProcessor('name',pipeline,dlqConfig)
  DLQ Config is optional

sp.name.start()

sp.name.stop()

sp.name.stats()

sp.listStreamProcessors()



#enable col for fullDocuments in given database

var cols = db.getCollectionNames()

for (const el of cols){
    db.runCommand( {
        collMod: el,
        changeStreamPreAndPostImages: { enabled: true }
    } )
}
