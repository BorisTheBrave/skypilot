"""
Launch a diamond-shaped DAG job using the SkyPilot Python SDK.

Topology:

        prep
       /    \\
   train_a  train_b      (train_a and train_b run concurrently)
       \\    /
        eval               (starts only after both trainers succeed)

Each task runs on its own cluster. `eval` is gated on both `train_a`
and `train_b` succeeding. If any upstream task fails, its downstream
tasks are marked CANCELLED with a failure_reason naming the upstream.

Usage:
    python examples/dag-sdk/simple_dag.py
"""
import sky

prep = sky.Task(
    name='prep',
    run="""
echo "prep starting"
sleep 5
echo "prep done" > /tmp/prep.out
""",
)
prep.set_resources(sky.Resources(cpus='1+'))

train_a = sky.Task(
    name='train_a',
    run="""
echo "train_a starting"
sleep 20
echo "train_a done"
""",
)
train_a.set_resources(sky.Resources(cpus='1+'))

train_b = sky.Task(
    name='train_b',
    run="""
echo "train_b starting"
sleep 20
echo "train_b done"
""",
)
train_b.set_resources(sky.Resources(cpus='1+'))

eval_ = sky.Task(
    name='eval',
    run="""
echo "eval starting (both trainers completed)"
sleep 3
echo "eval done"
""",
)
eval_.set_resources(sky.Resources(cpus='1+'))

with sky.Dag() as dag:
    for t in (prep, train_a, train_b, eval_):
        dag.add(t)
    dag.add_edge(prep, train_a)
    dag.add_edge(prep, train_b)
    dag.add_edge(train_a, eval_)
    dag.add_edge(train_b, eval_)

dag.name = 'simple-dag'
dag.set_execution(sky.DagExecution.DAG)

sky.stream_and_get(sky.jobs.launch(dag))
