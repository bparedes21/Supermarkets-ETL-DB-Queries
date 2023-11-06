#precios_semana_20200413.csv
#https://drive.google.com/file/d/1cYv46Y1Kt1xRKJdJn0hXQtL7jbSd_rNr/view?usp=sharing

#precios_semana_20200503.json
#https://drive.google.com/file/d/1KwzPcILjFXnov317aZH5FGe10dnGiQ_r/view?usp=sharing

#precios_semana_20200518.txt
#https://drive.google.com/file/d/1AV8qQFLoiuX2yahyuE8PAjf5qXwdp4Qf/view?usp=sharing
#precios_semanas_20200419_20200426.xlsx
#https://docs.google.com/spreadsheets/d/16wYqUJX0X8tEcF7gzFxGVWOWsv-_5OIZ/edit?usp=sharing&ouid=118350918828854055251&rtpof=true&sd=true

#producto.parquet
#https://drive.google.com/file/d/1SL3dMdr9yFuq8xpLK05j9pZYnJUtwuon/view?usp=sharing

#sucursal.csv
#https://drive.google.com/file/d/1skuhvdTkgh_GZ9wL0FEf3uxSZu7wU-mX/view?usp=sharing


import pandas as pd

def generar_url_descarga_de_achivo(url):

    file_id=url.split('/')[-2]
    dwn_url='https://drive.google.com/uc?id=' + file_id
    return dwn_url

def crear_df(dwn_url,read_):
    storage_options = {'User-Agent': 'Mozilla/5.0'}
    if(read_=="csv"):
        ##sucursal.csv

        df = pd.read_csv(dwn_url ,storage_options=storage_options)
 
    if(read_=="json"):
        ##precios_semana_20200503.json
        df=pd.read_json(dwn_url ,storage_options=storage_options)

    elif(read_=="txt"):
        ##precios_semana_20200518.txt
        df=pd.read_csv(dwn_url,storage_options=storage_options, delimiter="|")
    elif(read_=="csv, encoding= UTF-16"):
        ##precios_semana_20200413.csv
        df = pd.read_csv(dwn_url ,storage_options=storage_options , encoding='UTF-16')
    elif(read_=="parquet"):
        df = pd.read_parquet(dwn_url)
    return df

def importar_todos_los_archivos():
    #archivo sucursal 
    #obtener ids del archivo
    """CSV"""
    url = "https://drive.google.com/file/d/1skuhvdTkgh_GZ9wL0FEf3uxSZu7wU-mX/view?usp=sharing"
    dwn_url0=generar_url_descarga_de_achivo(url)
    #archivo csv que descargo desde el drive
    sucursal_df =crear_df(dwn_url0,"csv")

    """PARQUET"""
    #archivo productos
    url1 = "https://drive.google.com/file/d/1SL3dMdr9yFuq8xpLK05j9pZYnJUtwuon/view?usp=sharing"
    dwn_url1=generar_url_descarga_de_achivo(url1)
    #archivo csv que descargo desde el drive
    productos_df =crear_df(dwn_url1,"parquet")
    """archivos de precios son 4"""

    ##archivo 1
    """CSV, ENCOING=UTF-16"""
    url2 = "https://drive.google.com/file/d/1cYv46Y1Kt1xRKJdJn0hXQtL7jbSd_rNr/view?usp=sharing"
    dwn_url2=generar_url_descarga_de_achivo(url2)
    #archivo csv que descargo desde el drive
    precios_semana_20200413_df =crear_df(dwn_url2,"csv, encoding= UTF-16")

    ##archivo 2
    """XLSX, 2 HOJAS"""
    
    #HOJA1
    sheet_id = "16wYqUJX0X8tEcF7gzFxGVWOWsv-_5OIZ"
    sheet_name = "precios_20200426_20200426"
    url3 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    precios_20200426_20200426_df =crear_df(url3,"csv")
        
    #HOJA2
    sheet_id = "16wYqUJX0X8tEcF7gzFxGVWOWsv-_5OIZ"
    sheet_name = "precios_20200419_20200419"
    url4 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    precios_20200419_20200419_df =crear_df(url4,"csv")

    """JSON"""
    url5 = "https://drive.google.com/file/d/1KwzPcILjFXnov317aZH5FGe10dnGiQ_r/view?usp=sharing"
    dwn_url5=generar_url_descarga_de_achivo(url5)
    #archivo csv que descargo desde el drive
    precios_semana_20200503_df =crear_df(dwn_url5,"json")

    """TXT"""
    url6 = "https://drive.google.com/file/d/1AV8qQFLoiuX2yahyuE8PAjf5qXwdp4Qf/view?usp=sharing"
    dwn_url6=generar_url_descarga_de_achivo(url6)
    #archivo csv que descargo desde el drive
    precios_semana_20200518_df =crear_df(dwn_url6,"txt")
    return sucursal_df,productos_df,precios_semana_20200413_df,precios_20200419_20200419_df,precios_20200426_20200426_df,precios_semana_20200503_df,precios_semana_20200518_df