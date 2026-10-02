"""Pull-based worker agent.

W1 slice: detect specs -> register once -> heartbeat forever. The agent ALWAYS
initiates contact; the control plane never connects out to it. Docker execution
of assigned runs arrives in W2.
"""
