import pyodbc
import pandas as pd
#%%
class DB_SQL_Server:
    def __init__(self,server,db,admin=None,pswd=None,driver = '17'):
        self.server = server
        self.db = db
        self.admin = admin
        self.pswd = pswd
        self.driver = driver
        self.cnxn = None
        self.cursor = None
        
  
    def Connect_db(self):
        try:
            if self.admin != None:
                self.cnxn = pyodbc.connect(
                r'DRIVER={ODBC Driver '+self.driver+' for SQL Server};'
                fr'SERVER={self.server};'
                fr'DATABASE={self.db};'
                r'ENCRYPT=no;'
                fr'UID={self.admin};'
                fr'PWD={self.pswd}')
            else:
                self.cnxn = pyodbc.connect(
                r'DRIVER={ODBC Driver '+self.driver+' for SQL Server};'
                fr'SERVER={self.server};'
                fr'DATABASE={self.db};'
                r'Trusted_Connection=yes;')
                
            self.cursor = self.cnxn.cursor()
            #print("CONEXION EXITOSA")
        except Exception as ex:
            print('No se pudo conectar a la base de datos:',ex)
    
    def Close_db(self):
        #print("CERRANDO CONEXION")
        self.cnxn.close()
        
    def GET_ROWS_db(self,sql_query):
        self.cursor.execute(sql_query)
        rows = self.cursor.fetchall()
        return rows
            
    def GET_DF_db(self,sql_query):
        rows = self.GET_ROWS_db(sql_query)
        num_columns = len(rows[0])
        columns = [i for i in range(num_columns)]
        df = pd.DataFrame(columns = columns)
        for row in rows:
            lrow = []
            for i in range(num_columns):
                if str(type(row[i])) == "<class 'datetime.datetime'>":
                    lrow.append(str(row[i])[:23])
                else:
                    lrow.append(str(row[i]))
            print(lrow)
            df.loc[df.shape[0]] = lrow
        return df
            
    def COMMIT_TABLE(self,sql_query):
        self.cursor.execute(sql_query)
        self.cursor.commit()
          
    
    def GET_ONE_ROW_db(self,sql_query):
        self.cursor.execute(sql_query)
        row = self.cursor.fetchone()
        return row[0]
        
    
    def _generate_camp_names(self,columns):
        tam = len(columns)
        camp_names = '('
        for i in range(tam):
            if i == tam-1:
                camp_names = camp_names+columns[i]
            else:
                camp_names = camp_names+columns[i] + ','
        camp_names = camp_names+')'
        return camp_names
    
    
    def _generate_values(self,lrow):
        values = '('
        for elem in lrow:
            if elem == 'None':
                values = values+'NULL'+','
            else:
                #elem = re.sub("'"," ",elem)
                values = values + f'{repr(elem)}'+','
        values = values.strip(',')
        values = values + ')'
        return values
        

    def STORAGE_ROWS_db(self,sql_create,columns,rows,table_name,ADD_NEW_ROWS=False):
        #Creacion de tabla
        if not ADD_NEW_ROWS:
            self.cursor.execute(sql_create)
        self.cursor.commit()
        #oBTENER NPOMBRE DE TABLA 
        num_columns = len(columns)
        #GENERANDO LOS NOMBRES DE CAMPO
        camp_names = self._generate_camp_names(columns)
        #Almacenamiento de datos en dicha tabla
        #Obtener la fila 
        print("ALMACENANDO LOS DATOS......ESPERAR UNOS MINUTOS")
        for num_index,row in enumerate(rows):
            lrow = []
            for i in range(num_columns):
                if str(type(row[i])) == "<class 'datetime.datetime'>":
                    lrow.append(str(row[i])[:23])
                else:
                    lrow.append(str(row[i]))
            #CONSTRUYUENDO LA QUERY !!!  
            values = self._generate_values(lrow)
            sql_insert = (
            fr'INSERT INTO {table_name} '
            fr'{camp_names} '
            r'values '
            fr'{values}'
                )
            try:
                self.cursor.execute(sql_insert)
                self.cursor.commit()
                #print("Agregando fila número:",num_index)
            except Exception as e:
                print("No se puedo subir el registro",num_index)
                print("Descripcion problema:",e)
                print(sql_insert)
                break
            

            
        