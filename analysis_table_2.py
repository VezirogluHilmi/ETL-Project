from airflow import DAG
import pendulum

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator



PROJE_AD = "tonal-nucleus-395310"
DB_AD = "EstateSales"


with DAG(
    dag_id="2_analiz",
    schedule="@daily",
    start_date=pendulum.datetime(2023,8,20,tz="UTC")
    ) as dag:


    sorgu =f"Select Serial_Number, List_Year from {PROJE_AD}.{DB_AD}.Sales"

    new_table_analysis_2 = BigQueryExecuteQueryOperator(
        task_id = "new_table_analysis_2",
        sql=sorgu,
        destination_dataset_table=f"{PROJE_AD}.{DB_AD}.analiz_tablo_2",
        create_disposition="CREATE_IF_NEEDED", 
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_conn"
    )


    new_table_analysis_2
