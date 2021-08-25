MQS (Message Queue Service) is a replacement for Alibaba MNS in little strict scenarios.
We have been using MNS in many scenarios. We spend more than 50¥ each day to Alibaba for MNS, most of them are not necessary.
So we create this project to replace MNS in some scenarios.

MQS support functions as follow:
## distributed 
product message and consume message deploy in different processes on different computers.

## cached
Messages should be cached until it's consumed or timeout.

## support delay message
Message can be delay time to be visible by consumers.

## at most once consumed
one message should be consumed once and at most once, unless timeout.
