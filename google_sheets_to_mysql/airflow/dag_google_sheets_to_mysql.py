from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from datetime import datetime, timedelta
import os
from slack_alert import slack_alert_da

def ventas_tienda_madrid_script(script_location):
    import sys
    sys.path.append(script_location)
    from ventas_madrid_actualizar_tabla import main
    main()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'execution_timeout': timedelta(hours=1)
}

dag = DAG(
    dag_id='ventas_tienda_madrid',
    description='Actualiza la tabla ventas_madrid',
    default_args=default_args,
    schedule_interval='0 11 * * *',
    tags=['data_analytics', 'ventas_tienda_madrid'],
    start_date=datetime(2025, 1, 9),
    catchup=False
)

ventas_tienda_madrid_task = PythonVirtualenvOperator(
    task_id='ventas_tienda_madrid_script',
    python_callable=ventas_tienda_madrid_script,
    python_version='3.10',
    requirements=['requirements.txt'],
    dag=dag,
    op_kwargs={'script_location': os.path.dirname(__file__)},
    on_failure_callback = slack_alert_da,
    on_success_callback = slack_alert_da,
    on_execute_callback = slack_alert_da
)