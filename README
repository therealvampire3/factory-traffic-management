# Factory Traffic Management System

### Backend Developer Intern Technical Assessment — CSI Smart Tech

An event-driven traffic signal management system for factory junctions. The system manages normal traffic, emergency vehicles, manual overrides, controller failures, persistence, and concurrent events while maintaining safe signal transitions.

---

## 1. Technology Stack

- **Python 3.10+**
- **SQLite**
- Python `http.server`
- Python `unittest`
- HTML / CSS / JavaScript
- Standard library only
- No external packages required

---

## 2. Quick Start

### Requirements

- Python **3.10 or newer**
- No external dependencies are required.

### Run the application

```bash
python run.py
```

The dashboard and API will be available at:

```text
http://localhost:8000
```

The database schema from `schema.sql` is applied automatically at startup.

### Run the tests

```bash
python -m unittest discover -s tests -t .
```

Current test result:

```text
Ran 27 tests
OK
```

---

## 3. Project Structure

```text
factory-traffic/
│
├── run.py
├── schema.sql
├── traffic.db
│
├── traffic/
│   ├── domain/
│   │   ├── engine.py
│   │   ├── errors.py
│   │   └── __init__.py
│   │
│   ├── adapters/
│   │   ├── simulator.py
│   │   └── __init__.py
│   │
│   ├── server.py
│   ├── service.py
│   ├── store.py
│   ├── ports.py
│   └── __init__.py
│
├── tests/
│   ├── test_engine.py
│   ├── test_service.py
│   └── __init__.py
│
└── README.md
```

---

# 4. System Architecture

The application follows a layered architecture that separates HTTP handling, application services, domain logic, persistence, and controller communication.

```text
                    HTTP / Dashboard
                           │
                           ▼
                  traffic/server.py
                           │
                           ▼
                 Application Service
                  traffic/service.py
                    │             │
                    ▼             ▼
             Domain Engine       SQLite
          traffic/domain/engine  store.py
                    │
                    ▼
              ControllerPort
                    │
                    ▼
           Simulated Controller
```

### Major Components

| Component | Responsibility |
|---|---|
| `server.py` | HTTP endpoints and dashboard/API serving |
| `service.py` | Application orchestration, locking, persistence and dispatch |
| `domain/engine.py` | Core traffic decisions and state transitions |
| `store.py` | SQLite persistence |
| `ports.py` | Controller interface |
| `adapters/simulator.py` | Simulated controller for testing and demonstrations |

### Domain Engine

The domain engine contains the core traffic-control logic and is kept independent from HTTP, SQLite, and real-time system calls.

Time is injected so the engine can be tested with a controlled clock.

The engine produces:

- Signal commands
- Audit information
- State snapshots

### Concurrency

Events belonging to the same junction are processed under a per-junction lock.

This applies to:

- Sensor events
- Manual commands
- Emergency events
- Controller events
- Timer ticks

This prevents concurrent operations from producing inconsistent junction state.

State, audit records, and commands are persisted in a SQLite transaction before commands are sent to the controller.

---

# 5. Traffic Signal Safety

The system never changes directly from one GREEN phase to another.

The normal transition sequence is:

```text
GREEN
  │
  ▼
YELLOW
  │
  ▼
ALL RED
  │
  ▼
GREEN
```

Before a phase can become GREEN, conflicting directions must be:

1. Desired to be RED
2. Confirmed RED by the controller

The same safety sequence is used for:

- Automatic traffic control
- Emergency preemption
- Manual overrides

This prevents a conflicting direction from being switched directly to GREEN.

### Operating Mode Priority

The effective priority is:

**DEGRADED > EMERGENCY > MANUAL > AUTOMATIC**

---

# 6. Automatic Traffic Scheduling

Each phase receives a score based on the vehicles waiting in that phase.

```text
Phase Score = Σ (Vehicle Priority + 0.1 × Waiting Time)
```

### Vehicle Priority

| Vehicle Type | Weight |
|---|---:|
| Emergency | 100 |
| Truck | 4 |
| Forklift | 3 |
| Employee Vehicle | 1 |

### Timing Parameters

| Parameter | Value |
|---|---:|
| Minimum green | 10 seconds |
| Maximum automatic green | 30 seconds |
| Yellow | 5 seconds |
| All-red | 2 seconds |
| Starvation threshold | 90 seconds |

The controller uses hysteresis to avoid unnecessary signal switching.

If the oldest vehicle in a phase has waited at least 90 seconds, that phase receives a starvation-prevention priority boost.

---

# 7. Emergency Vehicle Handling

When an emergency vehicle arrives while its phase is RED, the system performs a safe preemption:

```text
Current GREEN
      │
      ▼
    YELLOW
      │
      ▼
   ALL RED
      │
      ▼
Confirmed RED
      │
      ▼
Emergency Phase GREEN
```

The system never switches directly from a conflicting GREEN phase to the emergency GREEN phase.

Emergency mode has higher priority than manual and automatic operation.

An emergency vehicle is cleared when:

- A `VEHICLE_CLEARED` event is received, or
- The emergency becomes stale after 60 seconds without an update.

---

# 8. Manual Control

An administrator can request a manual GREEN phase for a direction.

Manual requests use the same safety transition:

```text
GREEN → YELLOW → ALL RED → GREEN
```

Manual overrides expire after **120 seconds** by default.

Emergency handling takes priority over manual control.

Manual requests during emergency or degraded operation are rejected.

---

# 9. Controller Failure & Recovery

The system supports controller acknowledgements and failures.

Controller commands can receive:

```text
ACK
NACK
FAILED
```

If a command does not receive an acknowledgement:

1. The command is retried up to 2 times.
2. The same `command_id` is reused.
3. After timeout, the system enters `DEGRADED` mode.
4. GREEN is not granted while degraded.
5. The system attempts to return the junction to an all-RED state.

When the controller reconnects, the physical signal state is treated as UNKNOWN and the system performs a safe resynchronization before allowing GREEN again.

---

# 10. Persistence & Restart Recovery

SQLite is used to persist:

- Junction configuration
- Traffic queues
- Processed event IDs
- Current mode and phase
- Desired signal states
- Actual signal states
- Emergency state
- Manual overrides
- Pending commands
- Audit history
- Controller information

### Restart Behavior

After a restart:

1. Application state is restored from SQLite.
2. Physical signal state becomes UNKNOWN.
3. Pending commands are abandoned.
4. A recovery event is recorded.
5. The junction enters the safe recovery sequence.
6. Signals move through YELLOW → ALL RED.
7. GREEN is allowed only after the required signal state is confirmed.

This prevents stale physical-state information from being used to immediately re-assert GREEN after a restart.

---

# 11. REST API

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/api/junctions` | List junctions |
| `GET` | `/api/junctions/:id` | Get junction configuration and status |
| `POST` | `/api/junctions` | Create a junction |
| `GET` | `/api/junctions/:id/status` | Get current traffic status |
| `POST` | `/api/sensor-events` | Process vehicle arrival/clearance |
| `POST` | `/api/junctions/:id/commands` | Send manual traffic commands |
| `POST` | `/api/controller-events` | Process controller ACK/status events |
| `GET` | `/api/junctions/:id/history` | View audit history |
| `GET/POST` | `/api/simulator` | Configure and inspect the simulator |

## Example: Vehicle Arrival

```bash
curl -X POST http://localhost:8000/api/sensor-events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id": "evt-1",
    "junction_id": "A",
    "direction": "EAST",
    "event_type": "VEHICLE_ARRIVED",
    "vehicle_id": "AMB-1",
    "vehicle_type": "EMERGENCY",
    "sequence_no": 1
  }'
```

## Example: Manual GREEN Request

```bash
curl -X POST http://localhost:8000/api/junctions/A/commands \
  -H 'Content-Type: application/json' \
  -d '{
    "command": "MANUAL_GREEN_REQUEST",
    "direction": "WEST"
  }'
```

---

# 12. Testing

Run the complete test suite with:

```bash
python -m unittest discover -s tests -t .
```

### Current Result

```text
Ran 27 tests
OK
```

The test suite covers:

- Traffic-state transitions
- Emergency handling
- Manual control
- Duplicate events
- Vehicle clearance
- Controller failures
- Restart recovery
- SQLite persistence
- HTTP endpoints
- Concurrent events

---

# 13. Demonstration Scenarios

The dashboard can be used to demonstrate:

1. Normal traffic and vehicle priority
2. Emergency vehicle preemption
3. Manual signal control
4. Duplicate event handling
5. Vehicle arrival and clearance
6. Controller failure and recovery
7. Restart and state recovery
8. Concurrent traffic events

---

# 14. Key Design Decisions

### Intent-Based API

Clients request actions instead of directly modifying traffic signal states.

The domain engine is responsible for deciding whether a requested action is safe.

### Per-Junction Locking

All operations affecting a junction are serialized to prevent race conditions.

### Idempotent Events

Duplicate event IDs are detected and do not create duplicate state changes.

Vehicle IDs are also used to prevent duplicate queue entries.

### Safe Recovery

After restart or controller reconnect, the physical signal state is treated as UNKNOWN.

GREEN is never restored purely from previously stored physical-state information.

### Fail-Safe Degraded Mode

When the controller is unavailable or inconsistent, the system does not grant GREEN.

### Audit Logging

Important operations, state changes, ignored events, and recovery actions are recorded for traceability.

---

# 15. Assumptions

- Different phases are treated as conflicting movements.
- `event_id` is used for event idempotency.
- `vehicle_id` prevents duplicate queue entries.
- Sensor timestamps are used for vehicle waiting time.
- Server time is used for timers and controller timeouts.
- Emergency vehicles become stale after 60 seconds without an update.
- Manual overrides expire after 120 seconds.
- Authentication and authorization are outside the assessment scope.
- MQTT integration is not implemented; the controller interface provides a seam for a future adapter.

---

# 16. Known Limitations & Future Improvements

Possible future improvements include:

- MQTT controller adapter
- Authentication and authorization
- Server-Sent Events or WebSockets for live updates
- Docker support
- Metrics and monitoring
- Event replay
- Per-vehicle maximum dwell time
- Persistent stage timers
- Detailed vehicle movement and turn modelling
- Pedestrian traffic support
- Independent hardware-level fail-safe monitoring

---

# 17. AI / Tool Usage

AI tools were used as development assistance during the assessment for code review, debugging, documentation, and validation.

The implementation was reviewed and tested by the candidate, who is responsible for understanding and explaining the architecture, backend logic, safety mechanisms, tests, and design decisions.
