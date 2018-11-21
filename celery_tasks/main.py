from celery import Celery
from store_to_mysql import mysql_store

# 创建celery应用
celery_app = Celery('store_data')

# 导入celery配置
celery_app.config_from_object('celery_tasks.config')

# 自动注册celery任务
celery_app.autodiscover_tasks(['celery_tasks.savetask'])

# 启动命令
# celery -A celery_tasks.main worker -l info -P eventlet




