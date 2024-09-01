from celery import Celery


app = Celery('tasks',
             broker='redis://localhost:6379/0',
             backend='redis://localhost:6379/0',
             )

app.conf.update(
    CELERY_ACCEPT_CONTENT=['application/json'],
    CELERY_TASK_SERIALIZER='json',
    CELERY_RESULT_SERIALIZER='json',
    CELERYD_LOG_FILE='worker.log',
    CELERYBEAT_LOG_FILE='beat.log',
)

