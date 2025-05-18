from transitions import Machine

class ScenarioStateMachine:
    states = ['init_startup', 'in_startup_processing', 'active',
              'init_shutdown', 'in_shutdown_processing', 'inactive']

    transitions = [
        {'trigger': 'start_processing', 'source': 'init_startup', 'dest': 'in_startup_processing'},
        {'trigger': 'activate', 'source': 'in_startup_processing', 'dest': 'active'},
        {'trigger': 'initiate_shutdown', 'source': 'active', 'dest': 'init_shutdown'},
        {'trigger': 'shutdown_processing', 'source': 'init_shutdown', 'dest': 'in_shutdown_processing'},
        {'trigger': 'deactivate', 'source': 'in_shutdown_processing', 'dest': 'inactive'},
    ]

    def __init__(self):
        self.machine = Machine(model=self, states=ScenarioStateMachine.states, transitions=ScenarioStateMachine.transitions, initial='init_startup')
