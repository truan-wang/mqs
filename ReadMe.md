MQS (Message Queue Service) is a replacment for Alibaba MNS in little strict area.
We have being using MNS in many scenarios, we spend more than 50¥ each day to Alibab for MNS, most of them are not necessary.
So we create this project to relpace MNS in some scenarios.
MQS will not be as robust as MNS, but shoud be much cheaper.

MQS support basic functions as follow:

## distributed 
product message and consume message deploy in different processes on different computers.

## cached
message should be cached until it's consumed or it's timeout.

## at most once consumed
one message should not be consumed more than once.