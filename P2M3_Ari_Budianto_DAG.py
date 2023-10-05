'''
===========================================================================================================================================
Project Optimize E-Commers Data Workflow With Airflow, Great Expectations and Kibana
Create by Ari Budianto
Aribudiantog3@gmail.com

Program ini dibuat untuk automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Dan dataset yang digunakan 
adalah E-Commerce Shipping Data yang di dapatkan dari kaggle. Bertujuan untuk Memberi informasi terhadap dataset kami dengan
menampilkan grafik dan mempermudah dalam memahami dataset yang berupa text. Serta menambahkan automasi dengan menggunakan airflow
agar scheduling dapat dilakukan setiap saat sesui yang diharapkan. 
==========================================================================================================================================
'''

# Import library
import pandas as pd
import psycopg2 as db
import datetime as dt
from airflow import DAG
from datetime import timedelta
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from airflow.operators.python_operator import PythonOperator


# Fungsi yang akan dijalankan untuk masing-masing task
def fetch_data_from_postgresql():
    # Membangun koneksi ke database
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow' port = '5432'"
    conn = db.connect(conn_string)

    # Membaca data dari database ke Pandas DataFrame
    query = "SELECT * FROM table_m3"
    df = pd.read_sql(query, conn)
    df.to_csv('/opt/airflow/dags/P2M3_Ari_Budianto_data_raw.csv')
    print("-------Data Saved------")


def clean_data():

    # loading hasil data
    df = pd.read_csv('/opt/airflow/dags/P2M3_Ari_Budianto_data_raw.csv')

    # Mengganti nama kolom
    df.rename(columns={'Reached.on.Time_Y.N': 'Reached_on_Time'}, inplace=True)

    # Mengubah nama kolom menjadi huruf kecil
    df.columns = df.columns.str.lower()
    df.to_csv('/opt/airflow/dags/P2M3_Ari_Budianto_data_clean.csv')


def post_to_kibana():
        # loading hasil data cleaning
        df= pd.read_csv('/opt/airflow/dags/P2M3_Ari_Budianto_data_clean.csv')

        # Konfigurasi koneksi ke Kibana/Elasticsearch
        es = Elasticsearch("http://elasticsearch:9200") 
        print('Connection Status',es.ping())

        # documents dan actions
        documents = df.to_dict(orient='records')
        actions = [
            {
                "_op_type": "index",  # Jenis operasi (indeks)
                "_index": "ajk",  # Nama indeks
                "_source": doc  # Isi dokumen (JSON)
            }
            for doc in documents
        ]

        # Menggunakan metode bulk untuk mengirimkan aksi indeks ke Elasticsearch
        response = helpers.bulk(es, actions)
        print(response)

# Parameter airflow
default_args = {
     'owner' : 'ari',
     'start_date' : dt.datetime(2023, 10, 4, 13, 55, 0) - dt.timedelta(hours=7), # hours=7 untuk menyamakan waktu dengan server
     'retries' : 1,
     'retry_delay' : dt.timedelta(minutes=5),
}

# Inisialisasi DAG
with DAG('my_dag', 
          default_args=default_args,
          schedule_interval=timedelta(minutes=5),
            ) as dag:
    
     # Task untuk mengambil data dari PostgreSQL
    fetch_task = PythonOperator(task_id='fetch_data',
                                python_callable=fetch_data_from_postgresql,
                              )



    # Task untuk membersihkan data
    clean_task = PythonOperator(task_id='clean_data',
                                python_callable=clean_data,

                            )

    # Task untuk mengirim data ke Kibana
    post_task = PythonOperator(
                                task_id='post_to_kibana',
                                python_callable=post_to_kibana,

                            )

# Mendefinisikan dependensi antar task
fetch_task >> clean_task >> post_task
 
