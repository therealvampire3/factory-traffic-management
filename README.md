**Factory Traffic Management System — Backend Developer Intern Assessment V2**  
Event-driven traffic-signal controller for factory junctions (Junction A seeded; more junctions via POST /api/junctions).  
   
 **Python 3.10+, standard library only** (HTTP server, SQLite, unittest) — nothing to install.  
**1. Setup / Run**  
python run.py                       # dashboard + API on http://localhost:8000  
 # env: PORT=8000  TRAFFIC_DB=traffic.db  AUTO_ACK=1 (0 = simulator does not auto-acknowledge)  
 python -m unittest discover -s tests -t .      # 27 tests (domain, concurrency, restart, HTTP)  
   
Open http://localhost:8000. Schema: schema.sql (applied automatically at startup).  
**2. Architecture (major decisions)**  
HTTP (traffic/server.py)  -->  Application service (traffic/service.py)  -->  Domain engine (traffic/domain/engine.py)  
                                    |  per-junction lock, persist, dispatch            pure logic: no HTTP/DB/MQTT/real time  
                                    +--> Store (SQLite, traffic/store.py)  
                                    +--> ControllerPort (traffic/ports.py) <-- SimulatedController (REST) / future MQTT adapter  
   
- **Domain engine is pure**: time is injected; outputs are outbox (commands), audit, snapshot. Tests run it with a fake clock and fake controller — no HTTP, browser, MQTT or DB.  
- **Consistency = serialised per-junction processing.** Every sensor event, admin command, controller event and timer tick takes the junction's lock, so decisions never interleave. State + audit + commands are written in  **one SQLite transaction before** commands are sent to the controller.  
- **Timers without sleep():** a 0.5 s background ticker calls engine.tick(now); request handlers never block.  
- **Clients send intents, never states.** No API writes a signal state. Only the engine produces signal commands, and _assert_safe() runs after every operation (a conflicting GREEN raises).  
- **Multiple junctions:** phases/directions/timings are per-junction config (POST /api/junctions), same engine class.  
- **Dashboard refresh: polling (1 s).** Simple, robust, state is tiny. SSE/WebSocket is the next step.  
- **MQTT:** not implemented. ControllerPort.send() plus a callback for acks/status is the seam; an MQTT adapter publishes commands (command_id as correlation id) and feeds acks into service.controller_event.  
**3. Traffic-state transitions**  
Per junction: stage in {GREEN, YELLOW, ALL_RED}, current phase, derived mode.  
GREEN(P) --want != P--> YELLOW(P) --(>=5s and YELLOW confirmed)--> ALL_RED --(>=2s and ALL signals CONFIRMED RED)--> GREEN(Q)  
   
- GREEN is granted only if every conflicting direction is **desired RED and actual (ACK-confirmed) RED**. Manual, emergency and automatic all use this same path.  
- **Mode** (derived, priority order): DEGRADED > EMERGENCY > MANUAL > AUTOMATIC.  
- **Startup / reconnect / restart:** physical state is UNKNOWN -> demand ALL RED (via YELLOW for the phase that may have been green) and wait for confirmation. GREEN is never re-asserted from old data.  
**4. Scheduling algorithm (AUTOMATIC)**  
Phase score = sum over waiting vehicles of type weight + 0.1 x seconds waited; weights EMERGENCY 100 > TRUCK 4 > FORKLIFT 3 > EMPLOYEE_VEHICLE 1 (so queue size, priority and waiting time all contribute).  
- Stay on the current green unless min_green (10 s) has elapsed **and** (the current phase has no demand,  **or** green >= 30 s,  **or** another phase scores > 1.25x the current one — hysteresis avoids needless switching).  
- **Starvation protection:** a phase whose oldest vehicle waited >= 90 s gets +1000 and preempts (after min-green).  
- The phase chosen when leaving green is remembered (next_phase) so a score change during YELLOW can't cancel the switch (bug found by the starvation test).  
- No demand anywhere -> stay on / return to the last phase (no switching when idle).  
- **Sensor OFFLINE:** that phase is assumed to have demand and gets time-based service.  
**5. API (JSON)**  
| | |  
|-|-|  
| **Method & path** | **Purpose / status codes** |   
| GET /api/junctions, GET /api/junctions/:id, POST /api/junctions | list statuses / config+status / create ({"junction_id":"B","phases":{"P1":["N"],"P2":["E","W"]}} + optional timings). 200 / 404 / 201 / 400 / 409 |   
| GET /api/junctions/:id/status | mode, phase, stage, controller_status, desired/actual signals, queues, emergencies, manual, pending commands, alerts |   
| POST /api/sensor-events | VEHICLE_ARRIVED / VEHICLE_CLEARED. **201** applied,  **200** duplicate,  **202** accepted but ignored (delayed / out-of-order / orphan), 400 malformed or unknown type, 404 unknown junction |   
| POST /api/junctions/:id/commands | MANUAL_GREEN_REQUEST {direction}, RETURN_TO_AUTOMATIC (optional actor). 202 / 400 / 409 (emergency or degraded) / 404 |   
| POST /api/controller-events | ack {command_id, junction_id, status: ACK/NACK/FAILED, actual_state}**or** status {junction_id, device_type: SIGNAL_CONTROLLER/SENSOR, direction?, status: ONLINE/OFFLINE/DEGRADED/WARNING/UNKNOWN, event_id?} |   
| GET /api/junctions/:id/history?limit=&event_type= | audit log, newest first |   
| GET/POST /api/simulator | {auto_ack, ack_delay} + recent commands "sent" (added endpoint) |   
   
**6. Persistence & recovery**  
Persisted (SQLite): junction config; full engine state per junction (queues, processed event ids (last 5000), mode, desired/actual, stage, emergency, manual, pending commands, timestamps); audit log; command table.  
   
 On restart: queues, processed ids, manual override (until expiry), emergencies (until stale), degraded flag and history are restored. **Actual state becomes UNKNOWN and pending commands become ABANDONED**; a RECOVERY audit entry is written; the junction is walked YELLOW -> ALL RED -> confirmed before any GREEN. Timers use wall-clock epoch seconds so they stay meaningful across restarts (stage timers restart at recovery).  
**7. Demonstrating the scenarios (dashboard "Traffic simulation" panel, or curl)**  
1. **Normal / priority:** add arrivals in several directions with different vehicle types; watch phase, queues and the activity log.  
2. **Emergency preemption:** "Emergency arrives" on a direction whose phase is red -> YELLOW -> ALL RED -> GREEN (never straight to green); mode EMERGENCY. Clear it with its vehicle id + "Vehicle cleared", or wait 60 s (stale timeout).  
3. **Manual:** "Request manual GREEN" -> same safe sequence; banner shows the override and expiry (120 s); "Return to automatic".  
4. **Duplicate:** "Resend last event" -> DUPLICATE, queue unchanged.  
5. **Clearance:** arrive, then clear with the same vehicle id; clearing twice or an unknown id is ignored; queue never < 0.  
6. **Controller failure:** untick *auto-ACK* -> commands stay pending (2 retries, same command_id) -> COMMAND_TIMEOUT -> DEGRADED, all RED, mismatch alerts. Or press  *Controller OFFLINE*. Recover with auto-ACK /  *Controller ONLINE*. Manual "ACK all pending" / "NACK first pending" also available.  
7. **Restart:** Ctrl-C and python run.py again -> queues and history remain, RECOVERY entry, signals re-confirmed starting from RED.  
8. **Concurrency:**tests/test_service.py::test_concurrent_events_stay_consistent (20 threads: trucks, duplicate emergencies, manual requests, ticks) and tests/test_engine.py::test_concurrent_sequence_scenario.  
curl -XPOST localhost:8000/api/sensor-events -H 'content-type: application/json' -d '{"event_id":"evt-1","junction_id":"A","direction":"EAST","event_type":"VEHICLE_ARRIVED","vehicle_id":"AMB-1","vehicle_type":"EMERGENCY","sequence_no":1,"timestamp":"'$(date -u +%FT%TZ)'"}'  
 curl -XPOST localhost:8000/api/junctions/A/commands -H 'content-type: application/json' -d '{"command":"MANUAL_GREEN_REQUEST","direction":"WEST"}'  
   
**Assumptions / Questions / Requirement Issues**  
| | |  
|-|-|  
| **Issue** | **Decision** |   
| **Safety rule 3 vs "immediately" serving emergency/manual** (looks contradictory) | Preemption *starts* immediately (YELLOW) but the target is GREEN only after YELLOW + ALL_RED + confirmed red. Min-green is waived for emergency/manual, never the clearance sequence. |   
| "Conflicting movement" undefined | Phases are conflict groups; any two different phases conflict. No turning-movement modelling. |   
| Which ID is authoritative? | event_id for idempotency (last 5000, survives restart). vehicle_id makes queue state idempotent even with new event_ids. sequence_no is only used to detect/log out-of-order delivery (spec doesn't say if it is per sensor or global; tracked per direction) and never drops events. |   
| Sensor vs server timestamp | Sensor time = vehicle arrival time (waiting time, ordering vs clearance; future times clamped to server time). Server time drives timers, timeouts and staleness. Events older than 10 min are rejected as delayed (202 + audit). |   
| Clearance without arrival | Ignored (202) and audited; a tombstone makes a *late older* arrival ignored too. A newer arrival is a legitimate new visit. Queue can't go negative. |   
| Vehicle types | Section 2 lists 5 categories, section 4 has 4 enum values. Only the 4 enums are accepted ("material-carrying" assumed = TRUCK); unknown -> 400. |   
| Repeated emergency events | Same vehicle refreshes last_seen; no extra queue entry. |   
| Competing emergencies from conflicting directions | First-come-first-served by sensor arrival time; the next is served after the first is cleared or stale. Same-phase emergencies share the green. |   
| Emergency ends when... | Its VEHICLE_CLEARED arrives, or no refresh for 60 s (stale: cleared and removed from queue, audited). |   
| Emergency vs manual | Emergency overrides manual. Manual requests during emergency/degraded -> 409. A stored manual override resumes after the emergency if not expired. |   
| Manual duration / two admins / disconnect | Expires after 120 s (configurable); no sessions, so disconnect is covered by expiry. Concurrent admins are serialised; **last command wins**, audited with actor / replaced_actor. No authentication (out of scope; actor is self-declared, unsafe in production). |   
| ACK timeout / retry | 5 s timeout, 2 retries with the **same command_id**, then TIMED_OUT -> actual UNKNOWN, controller UNKNOWN,  **DEGRADED**. |   
| Duplicate / late / unknown ACK | Duplicate -> ignored + audited. Unknown id -> ignored. ACK for a SUPERSEDED/ABANDONED command -> ignored (may carry stale state; never changes actual). Late ACK for a TIMED_OUT command that is still the desired state -> accepted. actual_state != requested, or NACK -> FAILED -> DEGRADED. |   
| Behaviour when degraded | Never grant GREEN. An active phase goes YELLOW -> RED without waiting for acks; all-RED re-sent every 10 s. Recovers automatically when the controller is ONLINE (valid ACK / ONLINE event) **and** all signals are confirmed RED. Real hardware needs an independent conflict monitor / flashing-red fail-safe; software alone cannot guarantee safety. |   
| Controller reconnect | Actual -> UNKNOWN, pending superseded, fail-safe resync (GREEN never re-asserted). |   
| Signal / sensor offline | Signal OFFLINE -> DEGRADED. Sensor OFFLINE -> alert + time-based fallback for that phase (queue untrusted). |   
| Queue = set of vehicles | Needs vehicle_id. Vehicles that never send CLEARED stay queued (emergencies expire); a production system would add a max-dwell expiry (not done). |   
| Signal timings | Green 30 s is the *max* green under automatic control (min 10 s); yellow 5 s; all-red 2 s; all configurable per junction. |   
| History SIGNAL_CHANGED example | Audit uses SIGNAL_REQUESTED (desired) and SIGNAL_CONFIRMED (actual) so the two states stay distinguishable. |   
| Simulator auto-ACK on by default | Demo convenience; one checkbox turns it off. |   
   
**Known limitations / next steps**  
MQTT adapter (with last-will for offline detection), SSE live updates, authN/Z for manual control, per-vehicle dwell expiry, stage timers that survive restarts, audit retention, event replay from the audit log, Docker, metrics, per-sensor sequence tracking, pedestrian/turn movements.  
**AI / Tool Usage**  
AI tools were used as development assistance for reviewing the implementation,  
debugging, improving documentation, and validating the solution. The candidate  
reviewed the implementation and is responsible for understanding and explaining  
the architecture, backend logic, and design decisions.  
