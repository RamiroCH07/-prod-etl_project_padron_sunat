from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from time import sleep

#COMPARACION DE TIEMPOS 
## FECHA ACTUAL
now = datetime.now().replace(microsecond = 0 )
## FECHA DE PROXIMA ACTUALIZACION
file = open('NEXT_DATE_UPDATE.txt')
next_update = datetime.strptime(file.read(),'%Y-%m-%d %H:%M:%S')
# instancializar objeto job
def print_time():
    print(datetime.now().replace(microsecond = 0))
sched = BackgroundScheduler()
pattern = '*/15'

if now > next_update:
    sched.add_job(print_time,'cron',second = pattern, next_run_time=datetime.now().replace(microsecond = 0),id = '1')
else:
    sched.add_job(print_time,'cron',second = pattern)
sched.start()

while True:
    next_update = str(sched.get_job('1').next_run_time)[:19]
    with open('NEXT_DATE_UPDATE.txt','w') as f:
        f.write(next_update)
    sleep(1)

