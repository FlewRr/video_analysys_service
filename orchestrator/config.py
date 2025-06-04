KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

SCENARIO_TOPIC = "scenario_topic"       # from API -> orchestrator, also from orchestrator -> runner
RUNNER_TOPIC = "runner_topic"           # orchestrator -> runner
PREDICTION_TOPIC = "prediction_topic"   # runner -> orchestrator

ALLOWED_STATES = [
    "init_startup",
    "in_startup_processing",
    "active",
    "init_shutdown",
    "in_shutdown_processing",
    "inactive"
]