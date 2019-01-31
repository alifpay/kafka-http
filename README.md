# kafka-http
Apache Kafka or any messaging system is typically used for asynchronous processing.
A requirement of implementing request/response paradigm on top of Apache Kafka.  
Solutions
1. Simple wait for response get from memory
2. Send response through websocket
3. Send response grpc stream
