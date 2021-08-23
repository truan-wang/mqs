Message struct:
  message_body: max size 64KB;
  priority: 1 - 16, higher priority get consume earily, default value 1;
  delay_seconds: delay to be consume, default value null, use queue default setting.

Message Resource: http://$host/api/message/$queueName/

# send message
POST ${Message Resource}

# get message for consume
GET ${Message Resource}

# consume message
DELETE ${Message Resource}/${messageID}

# modify message properties
PUT ${Message Resource}/${messageID}


Message Queue info struct:
  max_ttl: message max Time To Live, default value 15 * 24 * 3600
  max_process_seconds: max seconds to process message, if you haven't consume the message after max_process_seconds since got the message, the message will be active again, default value 60 seconds.
  delay_seconds: default delay setting, default value 0
  created_messages_count: readonly
  active_messages_count: readonly
  inactive_messages_count: readonly
  get_messages_count: readonly
  consumed_messages_count: readonly
  latest_worker_check_time: readonly

Message Queue info Resource: http://$host/api/message/$queueName/info

# get Message Queue Info
GET  ${Message Queue info Resource}

# modify Message Queue Info
PUT  ${Message Queue info Resource}
