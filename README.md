### NOTES 

Created 1 Kafka producer to send messages to a stream of kafka  topology consisting of 1 source node and 1 processor node which adds the received message to database 

The message sent by kafka producer is from a GET request to https://api.adviceslip.com/advice running on multiple threads.

Demo:
(./demo_vid.mov)

1. Run `docker-compose -f docker-compose.yml up` to start docker kafka and zookeeper container 




