import requests as req
from bs4 import BeautifulSoup as bs
import pandas as pd
#from time import sleep
#%%
class A_R_WEB_SITE:
     
    def __init__(self):
        self.ENDPOINT = 'https://ww3.sunat.gob.pe/descarga/AgentRet/'
        self.headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'}
    def __get_fech_act(self,soup):
        td_content = soup.find('td',class_ = 't1').get_text()
        return td_content.split("(")[1].strip(')').split(':')[1].strip()
    def GET_FECH_ACT(self):
        url = self.ENDPOINT+'AgenRet0.html'
        response = req.get(url,headers = self.headers)
        html = response.text
        soup = bs(html,'html.parser')
        fech_act = self.__get_fech_act(soup)
        return fech_act
    
    
      
    def _next_resource(self,soup):
        etiq_td = soup.find('td',align = 'right')
        #OBTENER EL VALOR DE ATRIBUTO DE UNA ETIQUETA
        try:
            etiq_a = etiq_td.find('a').attrs
            return etiq_a['href'],True
        except:
            return None,False
            
        
    def _get_rows(self,soup):
        df = pd.DataFrame(columns = [str(i) for i in range(5)])
        table = soup.find_all('table',cellpadding = '2')[1]
        trs = table.find_all('tr')
        fech_act = self.__get_fech_act(soup)
        for tr in trs:
            is_my_tr = len(tr.find_all('td',class_ = 'bg')) != 0
            if is_my_tr:
                elems = tr.find_all('td')
                ruc = elems[0].find('a').get_text()
                name = elems[1].get_text()
                date = elems[2].get_text()
                res = elems[3].get_text()
                df.loc[df.shape[0]] = [ruc,name,date,fech_act,res]
        #df = df[df.index % 2 == 0]
        return df
        
    
    def GET_ALL_ROWS_IN_DF(self,number_resource_start):
        this_continue = True 
        new_page = f'AgenRet{number_resource_start}.html'
        df = pd.DataFrame(columns = [str(i) for i in range(5)])
        print("Iniciando el proceso de CRAWILING......ESPERAR UNOS MINUTOS")
        while this_continue:
            url = self.ENDPOINT+new_page
            try:
                response = req.get(url,headers = self.headers, timeout = 2)
                print('Recurso recuperado:',new_page)
                html = response.text
                soup = bs(html,'html.parser')
            except:
                print('Nuevo intento de recuperación')
                continue
            ##RECUPERANDO LOS RUCS
            #print('RECUPERANDO DEL RECURSO NUMERO: '+ re.search('[0-9]+',new_page).group())
            df = pd.concat([df,self._get_rows(soup)])
            ##
            nextr = self._next_resource(soup)
            new_page = nextr[0]
            this_continue = nextr[1]
            #sleep(1)
        #df = df.dropna()
        df.index = [i for i in range(df.shape[0])]
        return df
  