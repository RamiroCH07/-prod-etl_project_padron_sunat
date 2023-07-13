from PadronSunat_Package.B_C_WEB_SITE import B_C_WEB_SITE
from PadronSunat_Package.A_R_WEB_SITE import A_R_WEB_SITE
from Trat_data import Trat_data
from DB_SQL_SERVER import DB_SQL_Server
import pandas as pd
from datetime import datetime
class INTEGRATE_ETL:
    def __init__(self,padron_type,server,db,admin,pswd):
        self.server = server
        self.db = db
        self.admin = admin
        self.pswd = pswd
        #self.names_table_destination = ['dbo.Buenos_Contribuyentes','dbo.Agentes_Retencion']
        if padron_type == 'bc':
            self.obj_extract = B_C_WEB_SITE()
            self.name_table_destination = 'Buenos_Contribuyentes'
        elif padron_type == 'ar':
            self.obj_extract = A_R_WEB_SITE()
            self.name_table_destination = 'Agentes_Retencion'
        self.obj_process = Trat_data()
        self.obj_database = DB_SQL_Server(self.server, self.db,self.admin,self.pswd)
  
    
    def test_get_fech(self):
        return self.obj_extract.GET_FECH_ACT()
    
    def test_commit_db(self,sql):
        self.obj_database.Connect_db()
        try:
            self.obj_database.COMMIT_TABLE(sql)
        except Exception as ex:
            print("ERROR AL REALIALIZAR EL COMMIT:",ex)
        self.obj_database.Close_db()
    
    
    def _is_there_new_update(self):
    
        fech_act = self.obj_extract.GET_FECH_ACT()
        fech_act = fech_act.split('/')
        fech_act = fech_act[2]+'-'+fech_act[1]+'-'+fech_act[0]
        sql_query = '''
        SELECT 
            MAX(Actualizado_al)
        FROM 
            {}
        '''.format(self.name_table_destination).strip()
        self.obj_database.Connect_db()
        last_fech_db = self.obj_database.GET_ONE_ROW_db(sql_query)
        self.obj_database.Close_db()
        last_fech_db = str(last_fech_db)
            
        fech_act = datetime.strptime(fech_act,'%Y-%m-%d').date()
        last_fech_db = datetime.strptime(last_fech_db,'%Y-%m-%d').date()
            
        if fech_act > last_fech_db:
            return True
        else:
            return False
        
        
    # Funcion que extrae los registros de la base de datos 
    # Funcion que extrae los ultimos registros subidos a la web y los trata
    def _get_new_df_from_web(self):
        df = self.obj_extract.GET_ALL_ROWS_IN_DF(0)
        df = self.obj_process.PREPROCESSING_ROWS(df)
        return df
    # Funcionalidad que genera una tabla aux para copiar todos los datos de Buenos_Contribuyentes
    def _generate_aux_table_in_db(self):
        self.mod_load.Connect_db()
        sql_query = '''
        DROP TABLE IF EXISTS AUX_TABLE
        SELECT
            RUC,
            Nombre,
            A_partir_del,
            Actualizado_al,
            Nro_Resolucion
        INTO
            AUX_TABLE
        FROM 
            {}
        '''.format(self.name_table_destination)
        self.obj_database.COMMIT_TABLE(sql_query)
    
    def _add_rows_extracted_in_db(self,df):
        columns = ['RUC','Nombre','A_partir_del','Actualizado_al','Nro_Resolucion']
        table_name = 'AUX_TABLE'
        self.obj_database.STORAGE_ROWS_db('', columns, df.values, table_name, ADD_NEW_ROWS=True)
    
    
    def _using_dense_rank_in_db(self):
        # Ya esta conectado a la base de datos
        sql_query = '''
        DROP TABLE IF EXISTS {0}
            SELECT *
            INTO
            {0}
            FROM
            (SELECT 
            	RUC,
            	Nombre,
            	A_partir_del,
            	Actualizado_al,
            	Nro_Resolucion,
            	DENSE_RANK () OVER (
            		PARTITION BY RUC
            		ORDER BY Actualizado_al DESC
            	) act_rank
            FROM
            	AUX_TABLE) ranked
            WHERE 
            	act_rank = 1;
        '''.format(self.name_table_destination).strip()
        self.obj_database.COMMIT_TABLE(sql_query)
    
    def _labeling_in_db(self):
        sql_query = '''
        DECLARE @Max_date DATE
        SET @Max_date = (SELECT MAX(Actualizado_al) FROM {0})
        DROP TABLE IF EXISTS AUX_TABLE
        SELECT *
        INTO 
        AUX_TABLE
        FROM
        (SELECT 
        	RUC,
        	Nombre,
        	A_partir_del,
        	Actualizado_al,
        	Nro_Resolucion,
        	CASE WHEN Actualizado_al = @Max_Date THEN 1 ELSE 0 END as Is_active
        FROM 
        	{0}) new_registers
        '''.format(self.name_table_destination).strip()
        self.obj_database.COMMIT_TABLE(sql_query)
        
    def _updating_maintable_in_db(self):
        sql_query = '''
        DROP TABLE IF EXISTS {0}
        SELECT *
        INTO 
        {0}
        FROM 
        AUX_TABLE;
        '''.format(self.name_table_destination).strip()
        self.obj_database.COMMIT_TABLE(sql_query)
    def _close_and_delete_aux_table_in_db(self):
        sql_query = '''
        DROP TABLE AUX_TABLE
        '''.strip()
        self.obj_database.COMMIT_TABLE(sql_query)
        self.obj_database.Close_db()
    
    def temporal_close_db(self):
        self.obj_database.Close_db()
        
    def _start_etl_process(self):
         # Extrayendo los datos de la web y preprocesamiento de ellos
         df = self._get_new_df_from_web()
         # Generamos una nueva tabla llamada "AUX_TABLE"(COMMIT EN la db)
         self._generate_aux_table_in_db()
         # Agregamos los datos extraidos a la tabla llamada 'AUX_TABLE'
         self._add_rows_extracted_in_db(df)
         # Generamos una consulta en base a Dense Rank y lo pasamos a la tabla Buenos_Contribuyenmts
         self._using_dense_rank_in_db()
         # Etiquetamos cada registro según el campo actualizado_al
         self._labeling_in_db()
         # Actualización final de la tabla Buenos_Contribuyentes
         self._updating_maintable_in_db()
         # Eliminar la tabla auxiliar y cerrar la base de datos
         self._close_and_delete_aux_table_in_db()
         
    
    
    def _start_etl_process_test(self):
        df = pd.read_csv('ROWS_2023.csv', encoding = 'utf-8-sig')
        df = self.mod_transf.PREPROCESSING_ROWS(df)
        self._generate_aux_table_in_db()
        # Agregamos los datos extraidos a la tabla llamada 'AUX_TABLE'
        self._add_rows_extracted_in_db(df)
        # Generamos una consulta en base a Dense Rank y lo pasamos a la tabla Buenos_Contribuyenmts
        self._using_dense_rank_in_db()
        # Etiquetamos cada registro según el campo actualizado_al
        self._labeling_in_db()
        # Actualización final de la tabla Buenos_Contribuyentes
        self._updating_maintable_in_db()
        # Eliminar la tabla auxiliar y cerrar la base de datos
        self._close_and_delete_aux_table_in_db()
        
    
    

    def temporal_start_table_modification(self):
        self.obj_database.Connect_db()
        self._using_dense_rank_in_db()
        # Etiquetamos cada registro según el campo actualizado_al
        self._labeling_in_db()
        # Actualización final de la tabla Buenos_Contribuyentes
        self._updating_maintable_in_db()
        # Eliminar la tabla auxiliar y cerrar la base de datos
        self._close_and_delete_aux_table_in_db()
        
           
    def temporal_get_all_rows_bc(self):
        df1 = self.obj_extract.GET_ALL_ROWS_IN_DF(0)
        print("Guardado de precaucion...")
        df1.to_csv('ROWS_2023.csv',index=False,encoding='utf-8-sig')
        df2 = self.obj_extract.GET_ALL_ROWS_IN_DF(2536)
        print("Guardado de precaucion...")
        df2.to_csv('ROWS_2021.csv',index=False,encoding='utf-8-sig')
        df3 = self.obj_extract.GET_ALL_ROWS_IN_DF(3907)
        print("Guardado de precaucion...")
        df3.to_csv('ROWS_2019.csv',index=False,encoding='utf-8-sig')
        
        df = pd.concat([df1,df2,df3])
        #Re-indexando
        df.index = [i for i in range(df.shape[0])]
        return df
    
    def temporal_get_all_rows_ar(self):
        df1 = self.obj_extract.GET_ALL_ROWS_IN_DF(0)
        print("Guardado de precaucion...")
        df1.to_csv('ROWS_2022_ar.csv',index=False,encoding='utf-8-sig')
        df2 = self.obj_extract.GET_ALL_ROWS_IN_DF(189)
        print("Guardado de precaucion...")
        df2.to_csv('ROWS_2016_ar.csv',index=False,encoding='utf-8-sig')
        df3 = self.obj_extract.GET_ALL_ROWS_IN_DF(190)
        print("Guardado de precaucion...")
        df3.to_csv('ROWS_2015_ar.csv',index=False,encoding='utf-8-sig')
        
        df = pd.concat([df1,df2,df3])
        df.index = [i for i in range(df.shape[0])]
        return df
  
    
    def temporal_preprocess_all_rows(self,df):
        df = self.obj_process.PREPROCESSING_ROWS(df)
        return df 
    
    
    def temporal_storage_all_rows(self,df):
        self.obj_database.Connect_db()
        sql_create = '''
        DROP TABLE IF EXISTS {0}
        CREATE TABLE {0}(
             RUC VARCHAR(100),
             Nombre VARCHAR(250),
             A_partir_del DATE,
             Actualizado_al DATE,
             Nro_Resolucion VARCHAR(100)
             ) 
        '''.format(self.name_table_destination).strip()
        columns = ['RUC','Nombre','A_partir_del','Actualizado_al','Nro_Resolucion']
        table_name = self.name_table_destination
        self.obj_database.STORAGE_ROWS_db(sql_create, columns, df.values, table_name)
        self.obj_database.Close_db()
        
       
    def FINAL_JOB_TEST(self):
        print("SE HA DETECTADO UNA NUEVA ACTUALIZACION. REALIZANDO LOS CAMBIOS")
        self._start_etl_process_test() 
        
       
    
    def FINAL_JOB(self):
        if self._is_there_new_update():
            print('NUEVOS REGISTROS AGREGADOS. EJECUTANDO EL PROCESO ETL')
            self._start_etl_process() 
        else:
            print('NO SE HAN AGREGADO NUEVOS REGISTROS EN EL SITIO WEB')
        
        
        