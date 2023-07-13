from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
#%%
class Printer_Next_Date:
    def __print_next_date(self,str_date):
        print('PROXIMA FECHA DE EJECUCION:',str_date)
        
    def PRINT_NEXT_TIME_DAYS(self,num_days):
        next_date = datetime.today().replace(microsecond=0) + timedelta(days = num_days)
        str_date = datetime.strftime(next_date,'%Y-%m-%d %H:%M:%S')
        self.__print_next_date(str_date)
    
    def PRINT_NEXT_TIME_SECONDS(self,num_seconds):
        next_date = datetime.today().replace(microsecond=0) + timedelta(seconds = num_seconds)
        str_date = datetime.strftime(next_date,'%Y-%m-%d %H:%M:%S')
        self.__print_next_date(str_date)
    
    def PRINT_NEXT_TIME_MINUTES(self,num_minutes):
        next_date = datetime.today().replace(microsecond=0) + timedelta(minutes = num_minutes)
        str_date = datetime.strftime(next_date,'%Y-%m-%d %H:%M:%S')
        self.__print_next_date(str_date)     
        
    def PRINT_NEXT_TIME_MONTHS(self,num_months):
        next_date = datetime.today().replace(microsecond=0) + relativedelta(months=+num_months)
        str_date = datetime.strftime(next_date,'%Y-%m-%d %H:%M:%S')
        self.__print_next_date(str_date)
