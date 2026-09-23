from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'

# Airflow's own run_id (e.g. "manual__2026-09-23T05:00:00+00:00" or
# "scheduled__2026-09-23T02:00:00+00:00") contains ':' and '+', which are not
# filesystem-safe and are rejected by src.extract.files._SAFE_RUN_ID. Strip them
# once here so every task in the run shares one sanitized, safe pipeline_run_id.
SANITIZED_RUN_ID = "{{ run_id | replace(':', '') | replace('+', '') }}"


def failure_callback(context):
    """Print stage/run/error context so a failed task is diagnosable from the logs
    alone. This is what Task 10.5 evidence ('failure callback output') captures."""
    ti = context['task_instance']
    print(
        'TASK FAILED: '
        f"dag={ti.dag_id} task={ti.task_id} "
        f"run_id={context['run_id']} try={ti.try_number}/{ti.max_tries + 1} "
        f"logical_date={context.get('logical_date')} "
        f"error={context.get('exception')!r} "
        f"log_url={ti.log_url}"
    )


DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=15),  # a hung task fails instead of blocking the DAG forever
    'on_failure_callback': failure_callback,
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    start_date=datetime(2026, 1, 1),
    # Daily at 02:00: source files are a static snapshot refreshed off-hours by
    # upstream systems, and running before business hours gives analysts a
    # curated dataset ready when the day starts, with room left for retries.
    schedule='0 2 * * *',
    # No historical backfill: data/source/ is a current snapshot, not a
    # date-partitioned history. Every catch-up run would re-extract the exact
    # same three files, so backfilling past days would not produce different
    # data and would only waste runs. Only the "as of today" run is meaningful.
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition']),
        'year': Param(2026, type='integer'),
        'month': Param(1, type='integer', minimum=1, maximum=12),
    },
    tags=['DSS150P'],
) as dag:
    extract = BashOperator(
        task_id='extract',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{SANITIZED_RUN_ID}" python -m src.cli extract',
    )
    transform = BashOperator(
        task_id='transform',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{SANITIZED_RUN_ID}" python -m src.cli transform',
    )
    # run_mode drives which loader runs, per Task 10.4: 'full' upserts the whole
    # curated set; 'partition' loads only the requested year/month and writes to
    # audit.partition_loads. Business logic still lives in src.cli / src.load,
    # not here -- this is just choosing which CLI command to invoke.
    load = BashOperator(
        task_id='load',
        bash_command=(
            f'cd {PROJECT} && PIPELINE_RUN_ID="{SANITIZED_RUN_ID}" '
            "{% if params.run_mode == 'full' %}"
            'python -m src.cli load'
            '{% else %}'
            'python -m src.cli load-partition '
            '--year {{ params.year }} --month {{ params.month }}'
            '{% endif %}'
        ),
    )
    validate = BashOperator(
        task_id='validate',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{SANITIZED_RUN_ID}" python -m src.cli validate',
    )

    extract >> transform >> load >> validate