from INTEGRATE_ETL import INTEGRATE_ETL
from PRINTER_NEXT_DATE import Printer_Next_Date
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from time import sleep


if __name__ == '__main__':

    #DATOS DE LA INSTANCIA DE BASE DE DATOS
    server = "es107dev"
    db = "DBBSCOMERCIALV5"
    admin = "desarrollo"
    pswd = "desarrollo"

    #COMPARACION DE TIEMPOS 
    ## FECHA ACTUAL
    now = datetime.now().replace(microsecond = 0 )
    ## FECHA DE PROXIMA ACTUALIZACION
    file = open('NEXT_DATE_UPDATE.txt')
    next_update = datetime.strptime(file.read(),'%Y-%m-%d %H:%M:%S')
    file.close()

    #
    #OBJETO BUENOS CONTRIBUYENTES
    obj_integrate_bc = INTEGRATE_ETL('bc',server, db, admin, pswd)
    #OBJETO AGENTES DE RETENCION
    obj_integrate_ar = INTEGRATE_ETL('ar',server, db, admin, pswd)
    
    sched = BackgroundScheduler()
    pattern = '1-12'

    #EJECUTAR JOB INMEDIATAMENTE
    if now > next_update:
    ##AGREGANDO JOB ETL PARA LOS BUENOS CONTRIBUYENTES
        sched.add_job(obj_integrate_bc.FINAL_JOB,
                        'cron',
                        month = pattern,
                        next_run_time=datetime.now().replace(microsecond = 0),
                        id = 'buenos_contribuyentes')
        
        ##AGERGANDO JOB ETL PARA LOS AGENTES DE RETENCION
        sched.add_job(obj_integrate_ar.FINAL_JOB,
                      'cron',
                       month = pattern,
                       next_run_time=datetime.now().replace(microsecond = 0),
                       id = 'agentes_retencion')
        
    else:
        ##AGREGANDO JOB ETL PARA LOS BUENOS CONTRIBUYENTES
        sched.add_job(obj_integrate_bc.FINAL_JOB,
                    'cron',
                    month = pattern,
                    id = 'buenos_contribuyentes')
        
        ##AGERGANDO JOB ETL PARA LOS AGENTES DE RETENCION
        sched.add_job(obj_integrate_ar.FINAL_JOB,
                    'cron',
                    month = pattern,
                    id = 'agentes_retencion')

    
    sched.start()


    while True:
        next_update = str(sched.get_job('buenos_contribuyentes').next_run_time)[:19]
        with open('NEXT_DATE_UPDATE.txt','w') as f:
            f.write(next_update)
        sleep(20)
    

