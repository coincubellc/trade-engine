import os
from celery import Celery
from trader import run_trader
from database import db_session

REDIS_URI = os.getenv('REDIS_URI')


app = Celery('celery_trader',
             backend=REDIS_URI,
             broker=REDIS_URI)

class SqlAlchemyTask(app.Task):
    """An abstract Celery Task that ensures that the connection the the
    database is closed on task completion"""
    abstract = True

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        db_session.close()
        db_session.remove()   


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        600, # every 10 minutes
        run_all_trader,
        name='trade cubes')

@app.task(base=SqlAlchemyTask)
def run_all_trader():
    run_trader()
