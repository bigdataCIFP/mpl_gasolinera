import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import datetime

url="https://www.clickgasoil.com/m/precio-de-gasolina-95-palma-de-mallorca"
doc=requests.get(url, timeout=25)
soup=BeautifulSoup(doc.text,'lxml')
seccion=soup.find_all('tbody')
dataset=[]
for benzinera in seccion[1].find_all('tr'):
    cell=benzinera.find_all('td')
    marca=cell[0].find('span').text
    direccion=cell[0].text[len(marca):]
    dataset.append([marca,direccion,cell[1].text[:-5]])
df=pd.DataFrame(dataset)
df['dia']=datetime.date.today()
df['preu']=pd.to_numeric(df[2])
dialect='mysql+pymysql://root@localhost:3306/gasolinera'
sqlEngine=create_engine(dialect)
dfLlistaBenzineres=pd.read_sql('llista',con=sqlEngine)
dfLlistaBenzineres.head()
dfJunt=df.merge(dfLlistaBenzineres,left_on=1,right_on="direccio",how="left")
dfEnviaSQL=dfJunt[["index","dia","preu"]]
dfEnviaSQL=dfEnviaSQL.set_index("index")
dfEnviaSQL.to_sql('valors', con=sqlEngine, if_exists='append')