Example end to end processors

For a very basic intro processor using the build in ASP sample data source see simple_solar

### accumulation_union_example

### array_explode
Takes a document field that is an array, checks that the array has a specific value within it, explodes (unwinds) the array into multiple records each with the field name now being equal to one of the values of the array, and unsets _id to avoid collisions 

### dynamicfilter

### packet_processor

### race_leaderboard

### race_leaderboard_changestreams

### simple_solar

### superdoc

### toplevel_arrayJSONExplode
