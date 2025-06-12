KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'

RUNNER_TOPIC = 'runner_topic'
PREDICTION_TOPIC = 'prediction_topic'
INFERENCE_TOPIC = 'inference_topic'

# Remove the HTTP endpoint since we're using Kafka now
# INFERENCE_URL = 'http://localhost:8003/predict'