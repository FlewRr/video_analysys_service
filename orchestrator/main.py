from fastapi import FastAPI
from fastapi_utils.tasks import repeat_every
from orchestrator.state_machine import ScenarioStateMachine
import requests

app = FastAPI()
state_machines = {}

@app.on_event("startup")
@repeat_every(seconds=2)  # Каждые 2 секунды проверяет статус сценариев
def background_transition_check():
    for scenario_id, state in scenarios.items():
        try:
            sm = state_machines[scenario_id]
            if sm.state == "init_startup":
                sm.transition("in_startup_processing")
                update_api_status(scenario_id, sm.state)
            elif sm.state == "in_startup_processing":
                sm.transition("active")
                update_api_status(scenario_id, sm.state)
            elif sm.state == "init_shutdown":
                sm.transition("in_shutdown_processing")
                update_api_status(scenario_id, sm.state)
            elif sm.state == "in_shutdown_processing":
                sm.transition("inactive")
                update_api_status(scenario_id, sm.state)
        except Exception as e:
            print(f"[Orchestrator] Ошибка перехода состояния {scenario_id}: {e}")



@app.post("/orchestrate/{scenario_id}/start")
def start_scenario(scenario_id: str):
    sm = ScenarioStateMachine("init_startup")
    state_machines[scenario_id] = sm
    sm.transition("in_startup_processing")
    update_api_status(scenario_id, sm.state)
    return {"scenario_id": scenario_id, "status": sm.state}


def update_api_status(scenario_id: str, new_status: str):
    try:
        response = requests.post(
            f"http://localhost:8000/scenario/{scenario_id}/",
            json={"status": new_status},
            timeout=5
        )
        response.raise_for_status()
    except Exception as e:
        print(f"[Orchestrator] Failed to update api status {scenario_id}: {e}")

@app.post("/orchestrate/{scenario_id}/start")
def start_scenario(scenario_id: str):
    sm = ScenarioStateMachine()
    sm.start_processing()
    sm.activate()
    scenarios[scenario_id] = sm.state

    update_api_status(scenario_id, sm.state)

    return {"scenario_id": scenario_id, "status": sm.state}

@app.post("/orchestrate/{scenario_id}/stop")
def stop_scenario(scenario_id: str):
    sm = ScenarioStateMachine()
    sm.initiate_shutdown()
    sm.shutdown_processing()
    sm.deactivate()
    scenarios[scenario_id] = sm.state

    update_api_status(scenario_id, sm.state)

    return {"scenario_id": scenario_id, "status": sm.state}
