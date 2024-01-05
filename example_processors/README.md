Example end to end processors

For a very basic intro processor using the build in ASP sample data source see simple_solar

### accumulation_union_example
Reads records from a Kafka topic with common keys but different fields and merges them into a single record in a tumblingWindow using $top before emitting back to a Kafka topic.

### array_explode
Takes a document field that is an array, checks that the array has a specific value within it, explodes (unwinds) the array into multiple records each with the field name now being equal to one of the values of the array, and unsets _id to avoid collisions 

### dynamicfilter
Reads a MongoDB change stream $source collection, performs a $lookup to find filtering rules and applys the filter, increments the value of the document and writes it back to the same collection creating a loop. The merge stage output collection is dynamic and breaks the loop after a specific value is reached, writing it to another collection. 

### packet_processor
Performs a tumblingWindow off of a kafka topic $source for packet combinations of src_ip, src_port, dst_ip, dst_port and performs some time math calculations. Also an example python script for collecting Packet data and writing it to a Kafka Topic 

### race_leaderboard
Calculates and updates a collection to be a leaderboard. Racer data comes from a Kafka Topic $source, the pace car is filter out, and then a window calculates the latest events over a window of time reducing the number of events that must be merged to the target leaderboard collection. Also includes a python script to generate kafka topic race car data

### race_leaderboard_changestreams
Like the race_leaderboard but designed for Change Stream Sources

### simple_solar
Uses the built in solar data source to perform validation filtering of iot solar devices, and windowing of the average wattage created before updating devices in a Mongo Collection

### superdoc
Two processors that work together to handle creation and updates, or deletes. The processors read the change stream $source of an entire database and output to a target collection a super document where each field of the document is the collection name it came from. Deletes work to detect that the source collection has deleted the document and removes it from the respective super document. 

### toplevel_arrayJSONExplode
2 Solutions for when a single record is a top level array json rather then a top level object according to the JSON RFC. Turns each array object into its own document for processing. 
