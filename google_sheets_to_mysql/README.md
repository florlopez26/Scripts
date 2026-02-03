# Ventas Tienda Madrid – ETL con Airflow

Pipeline ETL que sincroniza datos de ventas desde Google Sheets hacia una tabla MySQL mediante una estrategia de **full refresh**.

## Flujo
1. Lectura de Google Sheets usando service account
2. Limpieza y normalización de datos con pandas
3. Eliminación completa de la tabla destino
4. Inserción del snapshot actualizado
5. Orquestación diaria con Airflow

## Estructura
- `src/`: script principal de procesamiento
- `airflow/`: DAG de Airflow
- `requirements.txt`: dependencias del entorno

## Tecnologías
- Python
- Pandas / NumPy
- Google Sheets API
- AWS Secrets Manager
- MySQL / SQLAlchemy
- Apache Airflow

## Estrategia de carga
Se utiliza **DELETE FROM + INSERT** dado que el volumen de datos es reducido y se busca consistencia total del snapshot.

## Notas
Las credenciales y recursos sensibles se gestionan mediante AWS Secrets Manager y no se incluyen en el repositorio.
