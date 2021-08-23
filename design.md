Key designs:

## use Redis to save/cache messages
  -- each message has one key(message ID) with value(message body), with ttl to delete message automaticly
  -- each message queue has map to save info
  -- each message queue has one active message ID list
  -- each message queue has one inactive message ID sorted set

## post new message to queue
  -- if message with delay, send to inactive message sorted set, with socre equal to message active timestamp
  -- else append to active message ID list

## get message to consume
  -- bpop message ID from active message ID list with timeout
     -- if no message popped, return null to consumer
     -- else add message ID to inactive message set with next active timestamp as score, return message to consumer

## consume message
  -- delete message only, don't modify message queue list or set

## Worker: inactive message to active message 
  -- set queue worker check time to now
  -- get one message ID with socre less than current timestamp in inactive message set.
  -- if no messag ID found, sleep 1 second, goto first step
  -- if message exist, append Message ID to active message list

## Worker Manager: make one and only one worker process one queue
  -- san queue map, if queue worker check time not exist or timeout, start new worker
  -- TODO: distribute workers into multiple process or machine, currently, all workers live with worker manage in single process is ok.
