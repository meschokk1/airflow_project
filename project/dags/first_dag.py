from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime



def extract_data():
    import requests
    '''Вытягиваем данные с url'''
    url = 'https://raw.githubusercontent.com/Hipo/university-domains-list/master/world_universities_and_domains.json'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception(f'Ошибка при загрузке данных: {response.status_code}')

def transform_data(**kwargs):
    '''Убираем лишние столбцы и добавляем категорию учебного заведения с помощью регулярных выражений'''
    import pandas as pd
    import re
    import numpy as np
    data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    df = pd.DataFrame(data)
    df.drop(columns=["domains", "web_pages"], inplace=True)

    def define_type(name):
        institution_types = ['College', "Institute", "University"]
        for institution in institution_types:
            if re.search(institution, name, re.IGNORECASE):
                return institution
        return np.NaN

    df['type'] = df['name'].apply(define_type)
    return df

def load_data():
    '''Загружаем данные в БД'''
    import pandas as pd
    from sqlalchemy import create_engine
    connection_string = 'postgresql://airflow:airflow@postgres:5432/airflow'
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        # Создание таблицы, если она не существует
        connection.execute("""
                CREATE TABLE IF NOT EXISTS universities (
                    name TEXT,
                    alpha_two_code TEXT,
                    country TEXT,
                    state_province TEXT,
                    type TEXT
                )
            """)

        # Инкрементальная загрузка данных
        df = pd.read_sql_table('universities', con=connection)
        new_data = pd.DataFrame(extract_data())
        new_data.drop(columns=["domains", "web_pages"], inplace=True)
        df = pd.concat([df, new_data], ignore_index=True)
        df.to_sql('universities', con=connection, if_exists='replace', index=False)

with DAG(
    dag_id = "first_dag",
    description='this DAG allows us to extract data about universities, transform it and save to postgres DB',
    schedule_interval = '0 3 * * *', # задача выполняется ежедневно в 3 ночи
    default_args = {
        'owner': 'Mikhail',
        'start_date': datetime(2024, 5, 4),
        'retries': 1,
    },
    catchup = False
) as dag:
    extract_task = PythonOperator(task_id='extract_data',python_callable=extract_data)
    transform_task = PythonOperator(task_id='transform_data',python_callable=transform_data,provide_context=True)
    load_task = PythonOperator(task_id = 'load_data', python_callable = load_data)
    extract_task >> transform_task >> load_task