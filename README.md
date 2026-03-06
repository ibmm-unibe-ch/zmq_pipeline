# ZMQ Pipeline
ZMQ Pipeline Framework is a distributed processing backbone built on [PyZMQ](https://pyzmq.readthedocs.io/), designed for server and HPC environments where large batches of independent work units must be processed in parallel across many compute nodes.

The framework implements the classic **Ventilator → Workers → Sink** fan-out/fan-in topology from the ZeroMQ Guide, extended with a central **Controller** that acts as the command and observability hub, and a web **Frontend** that serves as both the job submission interface and a real-time monitoring dashboard.

**The framework intentionally contains no processing logic.** It is a pure infrastructure backbone. Domain-specific pipelines are built by implementing worker handlers that plug into the framework.


### When to Use This Framework

This framework is the right choice when:
- You have a large batch of **independent, embarrassingly-parallel** work units (no inter-task dependencies)
- Results are **numpy array dictionaries** — the binary framing protocol is optimised for this shape
- You need to run Workers across **multiple compute nodes** (HPC cluster, cloud VMs) with minimal coordination
- You want **real-time observability** of a running job from a web browser without polling

This framework is **not** the right choice when:
- Tasks have complex dependencies (use a DAG scheduler: Airflow, Prefect, or Dask)
- Tasks need to communicate with each other (use MPI or Ray)
- You need durable job queuing with automatic retry across restarts (use Celery + Redis/RabbitMQ)


### Installation
```bash
git clone https://github.com/ibmm-unibe-ch/zmq_pipeline.git
pip install -e .
```

### Example
You can run the demo example with:
```
python run_demo.py
```


## End-to-End Job Flow
```
Frontend          Controller         Ventilator           Workers (N)          Sink
   │                  │                   │                    │                 │
   │─── HTTP POST ───►│                   │                    │                 │
   │   (job JSON)     │                   │                    │                 │
   │                  │── validate ──┐    │                    │                 │
   │                  │◄─────────────┘    │                    │                 │
   │                  │                   │                    │                 │
   │                  │─── REQ (job) ────►│                    │                 │
   │                  │◄── REP (ACK) ─────│                    │                 │
   │                  │                   │                    │                 │
   │◄── 202 Accepted ─│                   │─── PUSH (task 1) ─►│                 │
   │                  │                   │─── PUSH (task 2) ─►│                 │
   │                  │                   │                    │                 │
   │                  │◄── PUB (status: distributing) ─────────│                 │
   │◄── WS event ─────│                   │                    │                 │
   │                  │                   │                    │── PUSH result ─►│
   │                  │                   │                    │── PUSH result ─►│
   │                  │                   │                    │                 │
   │                  │◄──────────────────────── PUB (status, log) ──────────────│
   │◄── WS events ────│                   │                    │                 │
   │                  │                   │                    │                 │
   │                  │◄─────────────────────── PUB (log: job_complete) ─────────│
   │◄── WS job_complete│                  │                    │                 │
   │                  │                   │                    │                 │
```

## Glossary

| Term | Definition |
|---|---|
| **Job** | A unit of work submitted by the Frontend. Contains a `job_id` and a list of inputs. One job maps to many tasks. |
| **Task** | A single input from a job's `inputs` list. The atomic unit of work processed by one Worker. |
| **Task envelope** | The JSON wrapper added by the Ventilator around a raw task payload before pushing to a Worker. |
| **Ventilator** | ZeroMQ terminology for the fan-out producer in a PUSH/PULL pipeline. Named after a ventilator that pushes air outward. |
| **Sink** | ZeroMQ terminology for the fan-in consumer that collects all results. |
| **REQ/REP** | ZeroMQ synchronous request/reply socket pair. Used for the Controller ↔ Ventilator job dispatch handshake. |
| **PUSH/PULL** | ZeroMQ asynchronous pipeline socket pair with automatic load balancing. Used for task distribution and result collection. |
| **PUB/SUB** | ZeroMQ publish/subscribe socket pair. Used for telemetry. Subscribers filter by topic prefix. |
| **Multipart frame** | A ZMQ message composed of multiple discrete byte sequences (frames) delivered atomically. Used for result delivery to pair metadata with binary array data. |
| **CurveZMQ** | ZeroMQ's built-in authenticated encryption layer based on the NaCl Curve25519 algorithm. |
| **Telemetry** | The stream of status and log events published by all pipeline components to the Controller. |
| **Observability plane** | The half of the Controller responsible for collecting and forwarding telemetry to the Frontend. |
| **Command plane** | The half of the Controller responsible for job ingestion, validation, and dispatch to the Ventilator. |
| **Zero-copy** | Transferring data without copying it between memory buffers. ZMQ achieves this for large messages by passing a pointer to the buffer rather than duplicating its contents. |