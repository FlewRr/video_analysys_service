KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

RUNNER_TOPIC = "runner_topic"          # For messages from orchestrator -> runner
INFERENCE_TOPIC = "inference_topic"    # Runner sends frames here (or direct runner -> inference)
PREDICTION_TOPIC = "prediction_topic"  # Inference -> runner or orchestrator
