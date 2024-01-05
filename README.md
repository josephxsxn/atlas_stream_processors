# atlas_stream_processors
Three main examples are indexed below: code_snipplets which are example MQL solutions to specific problems, example_procesors are end to end processors, and terraform examples for reference. 

## code_snipplets
[Examples of MQL code to solve various problems](https://github.com/josephxsxn/atlas_stream_processors/tree/master/code_snipplets)


## example_processors
[Example end to end processors](https://github.com/josephxsxn/atlas_stream_processors/tree/master/example_processors)

## terraform
### [basicsample.data](https://github.com/josephxsxn/atlas_stream_processors/blob/master/terraform/basicsample.data)
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
