import os
import time
import logging
import threading
from datetime import datetime, timedelta
from storage import SessionLocal, get_scenario, update_scenario_state, Scenario
from kafka_producer import send_runner_command
from outbox import create_outbox_event

logger = logging.getLogger(__name__)

# Heartbeat timeout in seconds
HEARTBEAT_TIMEOUT = 30
# Maximum number of restart attempts
MAX_RESTART_ATTEMPTS = 3

class HeartbeatMonitor:
    def __init__(self):
        self.last_heartbeats = {}  # scenario_id -> last_heartbeat_time
        self.monitor_thread = None
        self.running = True
        self.restart_attempts = {}  # scenario_id -> number of restart attempts

    def update_heartbeat(self, scenario_id: str, service_id: str):
        """Update the last heartbeat time for a scenario and service"""
        current_time = datetime.utcnow()
        key = f"{scenario_id}:{service_id}"
        self.last_heartbeats[key] = current_time
        logger.info(f"[HeartbeatMonitor] Updated heartbeat for scenario {scenario_id} from service {service_id} at {current_time}")
        logger.info(f"[HeartbeatMonitor] Current heartbeat records: {self.last_heartbeats}")

    def check_heartbeats(self):
        """Check for missing heartbeats and restart scenarios if needed"""
        while self.running:
            try:
                current_time = datetime.utcnow()
                session = SessionLocal()
                
                # Get all active scenarios
                active_scenarios = session.query(Scenario).filter(
                    Scenario.state.in_(["active", "in_startup_processing", "in_shutdown_processing"])
                ).all()

                logger.info(f"[HeartbeatMonitor] Checking {len(active_scenarios)} active scenarios")
                logger.info(f"[HeartbeatMonitor] Current heartbeat records: {self.last_heartbeats}")

                # Required services that should send heartbeats
                required_services = ["runner", "inference"]

                for scenario in active_scenarios:
                    logger.info(f"[HeartbeatMonitor] Checking scenario {scenario.id} in state {scenario.state}")
                    
                    # Check if we have heartbeats from all required services
                    missing_services = []
                    for service in required_services:
                        key = f"{scenario.id}:{service}"
                        if key not in self.last_heartbeats:
                            missing_services.append(service)
                            continue
                        
                        # Check if heartbeat is too old
                        last_heartbeat = self.last_heartbeats[key]
                        time_since_last_heartbeat = current_time - last_heartbeat
                        logger.info(f"[HeartbeatMonitor] Time since last heartbeat for {key}: {time_since_last_heartbeat.total_seconds()} seconds")
                        
                        if time_since_last_heartbeat > timedelta(seconds=HEARTBEAT_TIMEOUT):
                            missing_services.append(service)
                    
                    if missing_services:
                        logger.warning(f"[HeartbeatMonitor] Missing heartbeats for scenario {scenario.id} from services: {missing_services}")
                        self._handle_scenario_failure(session, scenario.id)
                        continue

                session.close()
                time.sleep(5)  # Check every 5 seconds
            except Exception as e:
                logger.error(f"[HeartbeatMonitor] Error in heartbeat check: {str(e)}")
                time.sleep(5)

    def _handle_scenario_failure(self, session, scenario_id: str):
        """Handle a scenario failure, either restarting it or marking it as inactive"""
        # Clean up heartbeat records for this scenario
        keys_to_remove = [k for k in self.last_heartbeats.keys() if k.startswith(f"{scenario_id}:")]
        for key in keys_to_remove:
            del self.last_heartbeats[key]
        logger.info(f"[HeartbeatMonitor] Cleaned up heartbeat records for scenario {scenario_id}")

        # Increment restart attempts
        self.restart_attempts[scenario_id] = self.restart_attempts.get(scenario_id, 0) + 1
        
        if self.restart_attempts[scenario_id] > MAX_RESTART_ATTEMPTS:
            # Too many restart attempts, mark as inactive
            logger.error(f"[HeartbeatMonitor] Scenario {scenario_id} marked as inactive after {MAX_RESTART_ATTEMPTS} restart attempts")
            
            # First send shutdown command to stop all services
            try:
                send_runner_command({
                    "type": "shutdown",
                    "scenario_id": scenario_id
                })
                # Wait a bit for shutdown to complete
                time.sleep(5)
            except Exception as e:
                logger.error(f"[HeartbeatMonitor] Error sending shutdown command for scenario {scenario_id}: {str(e)}")
            
            # Then mark as inactive
            update_scenario_state(session, scenario_id, "inactive")
            create_outbox_event(
                event_type='scenario_state_changed',
                payload={
                    "scenario_id": scenario_id,
                    "new_state": "inactive",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            # Clean up restart attempts
            del self.restart_attempts[scenario_id]
        else:
            # Try to restart the scenario
            self._restart_scenario(session, scenario_id)

    def _restart_scenario(self, session, scenario_id: str):
        """Restart a scenario by shutting it down and starting it again"""
        logger.info(f"[HeartbeatMonitor] Trying to restart scenario {scenario_id}")
        try:
            # First, shut down the scenario
            update_scenario_state(session, scenario_id, "init_shutdown")
            create_outbox_event(
                event_type='scenario_state_changed',
                payload={
                    "scenario_id": scenario_id,
                    "new_state": "init_shutdown",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

            # Send shutdown command
            send_runner_command({
                "type": "shutdown",
                "scenario_id": scenario_id
            })

            # Wait a bit for shutdown to complete
            time.sleep(5)

            # Now restart the scenario
            scenario = get_scenario(session, scenario_id)
            if scenario:
                update_scenario_state(session, scenario_id, "init_startup")
                create_outbox_event(
                    event_type='scenario_state_changed',
                    payload={
                        "scenario_id": scenario_id,
                        "new_state": "init_startup",
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )


                update_scenario_state(session, scenario_id, "in_startup_processing")
                create_outbox_event(
                    event_type='scenario_state_changed',
                    payload={
                        "scenario_id": scenario_id,
                        "new_state": "in_startup_processing",
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )

                send_runner_command({
                    "type": "start",
                    "scenario_id": scenario_id,
                    "video_path": scenario.video_path
                })

                
                update_scenario_state(session, scenario_id, "active")
                create_outbox_event(
                    event_type='scenario_state_changed',
                    payload={
                        "scenario_id": scenario_id,
                        "new_state": "active",
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                logger.info(f"[HeartbeatMonitor] Restarted scenario {scenario_id} (attempt {self.restart_attempts[scenario_id]})")
        except Exception as e:
            logger.error(f"[HeartbeatMonitor] Error restarting scenario {scenario_id}: {str(e)}")

    def start(self):
        """Start the heartbeat monitoring thread"""
        self.monitor_thread = threading.Thread(target=self.check_heartbeats, daemon=True)
        self.monitor_thread.start()
        logger.info("[HeartbeatMonitor] Started heartbeat monitoring")

    def stop(self):
        """Stop the heartbeat monitoring thread"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("[HeartbeatMonitor] Stopped heartbeat monitoring")

# Global instance
heartbeat_monitor = HeartbeatMonitor() 