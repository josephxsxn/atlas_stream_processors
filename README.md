# atlas_stream_processors
Three main examples are indexed below: code_snipplets which are example MQL solutions to specific problems, example_procesors are end to end processors, and terraform examples for reference. 

## code_snipplets
Examples of MQL code to solve various problems
#### cs_coll_to_object
When using a change stream _$source_ will take the name of the collection from which the document came from and create a new root document object named after the collection with the fullDocument as its value, also includes the fullDocument._id at the root level.
#### dedupe_whole_doc
Deduplcates identical documents in the window by using the $$ROOT object and setting it to an _id, the replace root in the pipeline then restores the original document structure, after which accumulators can be used without worrying about counting duplicates
#### distinct_count
Alternate ways of counting distinct occurrences in a window 
#### kafka_metadata
Kafka metadata which a _$source_ provices in _stream_meta
#### related_record_accumulation
Ways to combine messages that have different fields but a common matching key together in a window by using $top to test if the object is missing or null 
#### source_sinks
Examples of different ways to configure $source, $merge, and $emit

## example_processors
Example end to end processors 
### explode_array_2_many_records
Takes a document field that is an array, checks that the array has a specific value within it, explodes (unwinds) the array into multiple records each with the field name now being equal to one of the values of the array, and unsets _id to avoid collisions 

## terraform
### basicsample.data
A terraform example (and links to docs) that will deploy an SPI (stream processing instance) and create a connection to an Atlas Database




# How To Enable Change Stream Sources
Enable col for fullDocuments in given database, the below code will enable ALL collections in the database it is ran in.

```
var cols = db.getCollectionNames()

for (const el of cols){
    db.runCommand( {
        collMod: el,
        changeStreamPreAndPostImages: { enabled: true }
    } )
}
```
