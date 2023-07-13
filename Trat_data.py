import re

class Trat_data:
    def _convert_date(self,date):
        new_date = date.split('/')
        year = new_date[2]
        month = new_date[1]
        day = new_date[0]
        new_date = f'{year}-{month}-{day}'
        return new_date
    
    def _drop_quo(self,name):
        new_name = re.sub('"','',name)
        new_name = re.sub("'"," ",new_name)
        new_name = new_name.strip()
        return new_name
    
    def PREPROCESSING_ROWS(self,df):
        df['1'] = df['1'].map(self._drop_quo) #nombre
        df['2'] = df['2'].map(self._convert_date) #
        df['3'] = df['3'].map(self._convert_date)
        return df
        
        
        
        

