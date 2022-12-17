##### KAFKA #####
# Go inside producer container instance
docker exec -it rtp-postgres-1 bash

# Go inside container container instance
docker exec -it rtp-consumer-1 bash

# Create topic in Kafka 
docker exec -d rtp-kafka-1 bash -c '/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic $KAFKA_TOPIC --bootstrap-server localhost:9092'

# Producer (Sample)
docker exec -it rtp-producer-1 bash -c 'python3 main.py --worker 5 --bootstrap-server $KAFKA_HOST --topic $KAFKA_TOPIC'

# Consumer (Sample)
docker exec rtp-consumer-1 bash -c 'python main.py --bootstrap-server $KAFKA_HOST --topic $KAFKA_TOPIC --tablename currency