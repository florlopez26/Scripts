#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import gspread
import pandas as pd
import boto3
import json
import numpy as np
import warnings

from google.oauth2 import service_account
from sqlalchemy.engine import create_engine, URL
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

warnings.filterwarnings('ignore', category=FutureWarning)


# =============================================================================
# DB connector (AWS Secrets Manager -> SQLAlchemy engine)
# =============================================================================
class DatabaseConnector:
    def __init__(self, secret_id, region="us-east-1"):
        self.secret_id = secret_id
        self.region = region
        self.engine = None
        self.current_server = None

    def get_secret(self, server):
        secrets_manager_client = boto3.client(service_name='secretsmanager', region_name=self.region)
        response = secrets_manager_client.get_secret_value(SecretId=self.secret_id)
        secret_json = json.loads(response['SecretString'])
        return json.loads(secret_json[server])

    def connect(self, server):
        if self.current_server != server:
            config = self.get_secret(server)
            url = URL.create(
                config['engine'],
                config['username'],
                config['password'],
                config['host'],
                config['port']
            )
            self.engine = create_engine(url)
            self.current_server = server
        return self.engine.connect()

    def execute_query(self, query, server):
        with self.connect(server) as connection:
            result = connection.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())


# =============================================================================
# Secrets + Google credentials
# =============================================================================
def load_secret_credentials(secretid: str, region="us-east-1"):
    secrets_manager_client = boto3.client(service_name='secretsmanager', region_name=region)
    try:
        response = secrets_manager_client.get_secret_value(SecretId=secretid)
        secret_json = response['SecretString'].encode().decode('unicode_escape')
        config = json.loads(secret_json)
        logging.info(f"Credenciales cargadas exitosamente desde {secretid}.")
        return config
    except Exception as e:
        logging.error(f"Error al obtener el secreto: {e}", exc_info=True)
        raise


def get_google_credentials(google_secret_id: str):
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    service_account_info = load_secret_credentials(google_secret_id)
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    return credentials


def get_google_sheet_data(credentials, spreadsheet_id, worksheet_name):
    try:
        gc = gspread.authorize(credentials)
        sh = gc.open_by_key(spreadsheet_id)
        wks = sh.worksheet(worksheet_name) if worksheet_name else sh.get_worksheet(0)
        data = wks.get_all_values()
        logging.info(f"Datos cargados correctamente desde la hoja '{worksheet_name}' (sheet_id='{spreadsheet_id}').")
        return data
    except Exception as e:
        logging.error(f"Error al obtener los datos de la hoja de cálculo: {e}", exc_info=True)
        raise


# =============================================================================
# Transform
# =============================================================================
def process_sales_data(data):
    df = pd.DataFrame(data[1:], columns=data[0])

    # Currency
    if 'importe_EUR' in df.columns:
        df['importe_EUR'] = (
            df['importe_EUR']
            .str.replace('€', '', regex=False)
            .str.replace(',', '', regex=False)
            .replace('', None)
            .astype(float)
            .round(2)
        )

    # IDs
    if 'id_cliente' in df.columns:
        df['id_cliente'] = df['id_cliente'].replace(r'^\s*$', np.nan, regex=True)
        df['id_cliente'] = pd.to_numeric(df['id_cliente'], errors='coerce').astype('Int64')

    if 'id' in df.columns:
        df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')

    # Dates
    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y', errors='coerce')

    if 'fecha_de_envio' in df.columns:
        df['fecha_de_envio'] = pd.to_datetime(df['fecha_de_envio'], format='%d/%m/%Y', errors='coerce')

    # Empty strings -> NaN for selected columns if they exist
    columnas_con_espacios_vacios = [
        'estado', 'tipo_de_venta', 'vendedor_email', 'nombre_cliente', 'telefono_cliente',
        'servicio_o_producto', 'linea_de_negocio', 'item_type', 'numero_de_pedido',
        'id_pago', 'id_orden', 'forma_de_pago', 'observaciones'
    ]
    cols_presentes = [c for c in columnas_con_espacios_vacios if c in df.columns]
    if cols_presentes:
        df[cols_presentes] = df[cols_presentes].replace(r'^\s*$', np.nan, regex=True)

    logging.info("Los datos se procesaron correctamente.")
    return df


# =============================================================================
# DB engine + load
# =============================================================================
def initialize_connections(secret_id_dbs, server_key, database):
    connector = DatabaseConnector(secret_id=secret_id_dbs)
    db_credentials = connector.get_secret(server_key)

    connection_url = URL.create(
        drivername=db_credentials['engine'],
        username=db_credentials['username'],
        password=db_credentials['password'],
        host=db_credentials['host'],
        database=database
    )

    engine = create_engine(connection_url)
    logging.info("Conexión al motor de base de datos establecida correctamente.")
    return engine


def save_to_db(dataframe, table_name, engine):
    # Delete old rows (full refresh)
    try:
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                conn.execute(text(f"DELETE FROM {table_name};"))
                trans.commit()
                logger.info("Datos antiguos eliminados exitosamente.")
            except Exception as e:
                trans.rollback()
                logger.error(f"Error al eliminar datos antiguos: {e}", exc_info=True)
                raise
    except Exception as e:
        logger.error(f"Error al establecer la conexión: {e}", exc_info=True)
        raise

    # Insert snapshot
    try:
        logger.info(f"Subiendo datos a la tabla {table_name}")
        dataframe.to_sql(table_name, engine, index=False, if_exists="append")
        logger.info("Datos subidos exitosamente.")
    except Exception as e:
        logger.error(f"Error al guardar los datos: {e}", exc_info=True)
        raise


# =============================================================================
# Main
# =============================================================================
def main():
    # --- Replace placeholders with your own values (kept generic for GitHub) ---
    google_secret_id = "google_service_account_secret"     # AWS secret id with Google SA JSON
    spreadsheet_id = "your_spreadsheet_id"                 # Google Sheet ID
    worksheet_name = "Sales 2025"                           # Worksheet name

    db_secret_id = "db_credentials_secret"                 # AWS secret id containing DB configs
    db_server_key = "mysql_reporting"                      # key inside the secret JSON
    db_name = "analytics_db"                               # database name
    target_table = "sales_table"                           # table name

    credentials = get_google_credentials(google_secret_id)
    data = get_google_sheet_data(credentials, spreadsheet_id, worksheet_name)

    df_sales = process_sales_data(data)

    engine = initialize_connections(db_secret_id, db_server_key, db_name)
    save_to_db(df_sales, target_table, engine)

    logger.info(f"Actualización de la tabla '{target_table}' completada.")


if __name__ == "__main__":
    main()
