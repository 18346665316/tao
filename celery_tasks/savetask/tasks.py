from celery_tasks.main import celery_app
from celery_tasks.main import mysql_store
import json


@celery_app.task(name='save_data')
def filter_and_store_mysql(text):
    text_dict = json.loads(text)
    mysql_store.run(text_dict)

