from INTEGRATE_ETL import INTEGRATE_ETL
from PRINTER_NEXT_DATE import Printer_Next_Date
from apscheduler.schedulers.blocking import BlockingScheduler


if __name__ == '__main__':
    server = "es107dev"
    db = "DBBSCOMERCIALV5"
    admin = "desarrollo"
    pswd = "desarrollo"
    obj_integrate_bc = INTEGRATE_ETL('bc',server, db, admin, pswd)
    obj_integrate_ar = INTEGRATE_ETL('ar',server, db, admin, pswd)
    obj_printer = Printer_Next_Date()
    num_units_time = 30
    start_date = '2023-07-13 10:00:00'
    def next_date():
        obj_printer.PRINT_NEXT_TIME_DAYS(num_units_time)
    
    sched = BlockingScheduler()
    ##AGREGANDO JOB ETL PARA LOS BUENOS CONTRIBUYENTES
    sched.add_job(obj_integrate_bc.FINAL_JOB,
                  'interval',
                  days = num_units_time,
                  start_date = start_date)
    
    ##AGERGANDO JOB ETL PARA LOS AGENTES DE RETENCION
    sched.add_job(obj_integrate_ar.FINAL_JOB,
                  'interval',
                  days = num_units_time,
                  start_date = start_date)
    
    ##AGREGANDO JOB DEL MENSAJE PROXIMA FECHA
    sched.add_job(next_date,
                  'interval',
                  days = num_units_time,
                  start_date = start_date)
    sched.start()
    
#%%
