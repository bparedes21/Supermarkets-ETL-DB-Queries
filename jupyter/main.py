import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sqlite3

def get_database_connection():
    
    mi_conexion=sqlite3.connect("miprimera.bd")
    return mi_conexion
def add_a_b(a, b, c):
            
            if (a==True or b==True or c==True):
                return 1
            else:
                return 0
def columna_boolean_nan(sheet1):
    COL1= sheet1["precio"].isnull()
    sheet1["bool1_precio"] = COL1
    COL2= sheet1["producto_id"].isnull()
    sheet1["bool2_producto_id"] = COL2
    COL3=sheet1["sucursal_id"].isnull()
    sheet1["bool3_sucursal_id"] = COL3
        
    sheet1["col_added"] = sheet1.apply(lambda x: add_a_b(x["bool1_precio"], x["bool2_producto_id"],x["bool3_sucursal_id"]), axis = 1)
    sheet1=sheet1.drop(columns=["bool1_precio","bool2_producto_id","bool3_sucursal_id"])
    return sheet1

def cortar_columna_sucursal_id_precio(df_precio):
    df_precio["sucursal_id"]=df_precio["sucursal_id"].astype('str')
    new2 = df_precio["sucursal_id"].str.split(" ", n = 1, expand = True)
    df_precio["sucursal_id"]=new2[0]
    return df_precio

def array_col_id_unicos(df_sucursal,nombre_de_columna):
    list=[]
    df_sucursal[nombre_de_columna]=df_sucursal[nombre_de_columna].astype('str')
    new2 = df_sucursal[nombre_de_columna].str.split("-", n = 2, expand = True)
    list=df_sucursal[nombre_de_columna]
    col_unicos=list.unique()
    return col_unicos

#saco el punto decimal que tiene la columna id producto
#completa con ceros a la izquierda
def borrar_nan_producto_id(sheet1):
    sheet1["producto_id"]=sheet1["producto_id"].astype('str')
    rep = {'NaN':'0.0','nan':'0.0',}
    sheet1["producto_id"] =sheet1["producto_id"].replace(rep, regex=True)
    new1= sheet1["producto_id"].str.split(".", n = 1, expand = True)
    sheet1["producto_id"]=new1[0]
    sheet1["producto_id"]= sheet1["producto_id"].str.zfill(13)
    return sheet1

#Tabla sucursales
mi_conexion=get_database_connection()
sucursal=pd.read_csv(r'C:\Users\ROXI\OneDrive\Escritorio\dataset procesados\Datasets relevamiento precios\sucursal.csv')
sucursal.to_sql('sucursales', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()
#Tabla productos
mi_conexion=get_database_connection()
producto_parquet=pd.read_csv(r'C:\Users\ROXI\OneDrive\Escritorio\dataset procesados\jupyter\producto_parquet_csv.csv')
sucursal.to_sql('productos', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()
#primer archivo a procesar e insertar en la base de datos
df_20200413=pd.DataFrame()
df_20200413=pd.read_csv(r'C:\Users\ROXI\OneDrive\Escritorio\dataset procesados\jupyter\precios_semana_20200413_csv.csv')
new_df=columna_boolean_nan(df_20200413)

new_df1=borrar_nan_producto_id(new_df)
sheet1=new_df1.copy()
sheet1["numero_archivo"]=1
mi_conexion=get_database_connection()
sheet1.to_sql('precios_temp', con=mi_conexion, index=False, index_label='id', if_exists='replace')

df_precios_=pd.read_sql("SELECT sucursal_id,producto_id,precio,numero_archivo FROM precios UNION ALL SELECT sucursal_id,producto_id,precio,numero_archivo FROM precios_temp", mi_conexion)

df_precios_.to_sql('precios', con=mi_conexion, index=False, index_label='id', if_exists='replace')


mi_conexion.commit()
mi_conexion.close()
df_final=sheet1[sheet1["col_added"]==0]
mi_conexion=get_database_connection()
df_final.to_sql('precios_final', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()
#Segundo archivo

#segundo archivo a procesar e insertar en la base de datos

sheet2=pd.DataFrame()
sheet2=pd.read_csv(r'C:\Users\ROXI\OneDrive\Escritorio\dataset procesados\jupyter\precios_semana_20200419_csv.csv')

sheet2["col_added"]=0
new_df=columna_boolean_nan(sheet2)
new_df1=borrar_nan_producto_id(new_df)
sheet2=new_df1.copy()
new_df1=cortar_columna_sucursal_id_precio(sheet2)
"""comprobar sucursalId si no coincide con id de la tabla sucursal se marca con 1 la columna col_added"""
list_suc_unicos=[]
list_suc_prec=[]
sheet2=new_df1.copy()
list_suc_unicos=array_col_id_unicos(sucursal,"id")
list_suc_prec=array_col_id_unicos(sheet2,"sucursal_id")

lis_bin=[]
lis_id=[]
for e in list_suc_prec:
    for i in list_suc_unicos:
        if(e == i):
            result_id=0
        else:
            result_id=1
            
        if (result_id == 0):
            break  
    lis_bin.append(result_id)
    lis_id.append(e)
list_zip=list(zip(lis_id,lis_bin))
DF=pd.DataFrame()
for e,num in enumerate(list_zip):
    #print(e,num[0],num[1])
    if(num[1]==1):
        DF=sheet2[sheet2["sucursal_id"]==num[0]]
        DF["col_added"]=1
        sheet2[sheet2["sucursal_id"]==num[0]]=DF
sheet2["numero_archivo"]=2
mi_conexion=get_database_connection()
sheet2.to_sql('precios_aux', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()
mi_conexion=get_database_connection()
sheet2.to_sql('precios_temp', con=mi_conexion, index=False, index_label='id', if_exists='replace')
df_precios_=pd.read_sql("SELECT sucursal_id,producto_id,precio,numero_archivo FROM precios UNION ALL SELECT sucursal_id,producto_id,precio,numero_archivo FROM precios_temp", mi_conexion)
df_precios_.to_sql('precios', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()


table = "CREATE TABLE precios_tb_final  as SELECT precios_final.sucursal_id,precios_final.producto_id,precios_final.precio,precios_final.col_added	,precios_final.numero_archivo FROM precios_final LEFT JOIN(SELECT * FROM precios_aux) AS prec USING(sucursal_id,producto_id)"
mi_conexion.execute(table)
mi_conexion.commit()
mi_conexion.execute("DROP TABLE IF EXISTS precios_tb_final")
table = "CREATE TABLE precios_tb_final  as SELECT precios_final.sucursal_id,precios_final.producto_id,precios_final.precio,precios_final.col_added	,precios_final.numero_archivo FROM precios_final LEFT JOIN(SELECT * FROM precios_aux) AS prec USING(sucursal_id,producto_id)"
mi_conexion.execute(table)
df_final=pd.read_sql("SELECT * FROM precios", mi_conexion)
mi_conexion.commit()
mi_conexion.execute("DROP TABLE IF EXISTS precios_final")
df_final.to_sql('precios_final', con=mi_conexion, index=False, index_label='id', if_exists='replace')


#tercer archivo a procesar e insertar en la base de datos

sheet1=pd.DataFrame()
sheet1=pd.read_csv(r'C:\Users\ROXI\OneDrive\Escritorio\dataset procesados\jupyter\precios_semana_20200426_csv.csv')

sheet1["col_added"]=0
new_df=pd.DataFrame()
new_df=columna_boolean_nan(sheet1)
"""cambia el valor nan por 000000000000 """
new_df1=pd.DataFrame()
sheet1=pd.DataFrame()
new_df1=borrar_nan_producto_id(new_df)
sheet1=new_df1.copy()
"""comprobar sucursalId si no coincide con id de la tabla sucursal"""
list_suc_unicos=[]
list_suc_prec=[]
list_suc_unicos=array_col_id_unicos(sucursal,"id")
list_suc_prec=array_col_id_unicos(sheet1,"sucursal_id")

lis_bin=[]
lis_id=[]
for e in list_suc_prec:
    for i in list_suc_unicos:
        if(e == i):
            result_id=0
        else:
            result_id=1
            
        if (result_id == 0):
            break  
    lis_bin.append(result_id)
    lis_id.append(e)
list_zip=list(zip(lis_id,lis_bin))
DF=pd.DataFrame()
for e,num in enumerate(list_zip):
    #print(e,num[0],num[1])
    if(num[1]==1):
        DF=sheet1[sheet1["sucursal_id"]==num[0]]
        DF["col_added"]=1
        sheet1[sheet1["sucursal_id"]==num[0]]=DF
sheet1["numero_archivo"]=3
mi_conexion=get_database_connection()
sheet1.to_sql('precios_aux', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()
mi_conexion=get_database_connection()

sheet1.to_sql('precios_temp', con=mi_conexion, index=False, index_label='id', if_exists='replace')
df_precios_=pd.DataFrame()
df_precios_=pd.read_sql("SELECT sucursal_id,producto_id,precio,numero_archivo FROM precios UNION ALL SELECT sucursal_id,producto_id,precio,numero_archivo FROM precios_aux", mi_conexion)
mi_conexion.execute("DROP TABLE IF EXISTS precios")
df_precios_.to_sql('precios', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()

mi_conexion.execute("DROP TABLE IF EXISTS precios_tb_final")
table = "CREATE TABLE precios_tb_final  as SELECT precios_final.sucursal_id,precios_final.producto_id,precios_final.precio,precios_final.col_added	,precios_final.numero_archivo FROM precios_final LEFT JOIN(SELECT * FROM precios_aux) AS prec USING(sucursal_id,producto_id)"
mi_conexion.execute(table)
df_final=pd.read_sql("SELECT * FROM precios", mi_conexion)
mi_conexion=get_database_connection()
df_final.to_sql('precios_final', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()
#cuarto archivo a procesar e insertar en la base de datos
df_20200503=pd.DataFrame()
df_20200503=pd.read_csv(r'C:\Users\ROXI\OneDrive\Escritorio\dataset procesados\jupyter\precios_semana_20200503_csv.csv')
new_df=pd.DataFrame()
new_df=columna_boolean_nan(df_20200503)
new_df1=pd.DataFrame()
new_df1=borrar_nan_producto_id(new_df)
sheet1=pd.DataFrame()
sheet1=new_df1.copy()
"""comprobar sucursalId si no coincide con id de la tabla sucursal se marca con 1 la columna col_added"""
list_suc_unicos=[]
list_suc_prec=[]

list_suc_unicos=array_col_id_unicos(sucursal,"id")
list_suc_prec=array_col_id_unicos(sheet1,"sucursal_id")

lis_bin=[]
lis_id=[]
for e in list_suc_prec:
    for i in list_suc_unicos:
        if(e == i):
            result_id=0
        else:
            result_id=1
            
        if (result_id == 0):
            break  
    lis_bin.append(result_id)
    lis_id.append(e)
list_zip=list(zip(lis_id,lis_bin))
DF=pd.DataFrame()
for e,num in enumerate(list_zip):
    #print(e,num[0],num[1])
    if(num[1]==1):
        DF=sheet1[sheet1["sucursal_id"]==num[0]]
        DF["col_added"]=1
        sheet1[sheet1["sucursal_id"]==num[0]]=DF
sheet1["numero_archivo"]=4
mi_conexion=get_database_connection()
sheet1.to_sql('precios', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()
mi_conexion=get_database_connection()
sheet1.to_sql('precios_aux', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()

mi_conexion=get_database_connection()
sheet1.to_sql('precios_temp', con=mi_conexion, index=False, index_label='id', if_exists='replace')
df_precios_=pd.DataFrame()
df_precios_=pd.read_sql("SELECT sucursal_id,producto_id,precio,numero_archivo FROM precios UNION ALL SELECT sucursal_id,producto_id,precio,numero_archivo FROM precios_temp", mi_conexion)
mi_conexion.execute("DROP TABLE IF EXISTS precios")
df_precios_.to_sql('precios', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()
mi_conexion.execute("DROP TABLE IF EXISTS precios_tb_final")
table = "CREATE TABLE precios_tb_final  as SELECT precios_final.sucursal_id,precios_final.producto_id,precios_final.precio,precios_final.col_added	,precios_final.numero_archivo FROM precios_final LEFT JOIN(SELECT * FROM precios_aux) AS prec USING(sucursal_id,producto_id)"
mi_conexion.execute(table)
mi_conexion=get_database_connection()
df_final=pd.read_sql("SELECT * FROM precios", mi_conexion)
df_final.to_sql('precios_final', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()
#quinto archivo a procesar e insertar en la base de datos
semana_20200518=pd.DataFrame()
semana_20200518=pd.read_csv(r'C:\Users\ROXI\OneDrive\Escritorio\dataset procesados\jupyter\precios_semana_20200518_csv.csv')
new_df=pd.DataFrame()
new_df=columna_boolean_nan(semana_20200518)
new_df1=pd.DataFrame()
new_df1=borrar_nan_producto_id(new_df)
sheet1=pd.DataFrame()
sheet1=new_df1.copy()
"""comprobar sucursalId si no coincide con id de la tabla sucursal se marca con 1 la columna col_added"""
list_suc_unicos=[]
list_suc_prec=[]

list_suc_unicos=array_col_id_unicos(sucursal,"id")
list_suc_prec=array_col_id_unicos(sheet1,"sucursal_id")

lis_bin=[]
lis_id=[]
for e in list_suc_prec:
    for i in list_suc_unicos:
        if(e == i):
            result_id=0
        else:
            result_id=1
            
        if (result_id == 0):
            break  
    lis_bin.append(result_id)
    lis_id.append(e)
list_zip=list(zip(lis_id,lis_bin))
DF=pd.DataFrame()
for e,num in enumerate(list_zip):
    #print(e,num[0],num[1])
    if(num[1]==1):
        DF=sheet1[sheet1["sucursal_id"]==num[0]]
        DF["col_added"]=1
        sheet1[sheet1["sucursal_id"]==num[0]]=DF
sheet1["numero_archivo"]=5
mi_conexion=get_database_connection()
mi_conexion.execute("DROP TABLE IF EXISTS precios")
sheet1.to_sql('precios', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()
mi_conexion=get_database_connection()
sheet1.to_sql('precios_aux', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()


mi_conexion=get_database_connection()
sheet1.to_sql('precios_temp', con=mi_conexion, index=False, index_label='id', if_exists='replace')
df_precios_=pd.DataFrame()
df_precios_=pd.read_sql("SELECT sucursal_id,producto_id,precio,numero_archivo FROM precios UNION ALL SELECT sucursal_id,producto_id,precio,numero_archivo FROM precios_temp", mi_conexion)
df_precios_.to_sql('precios', con=mi_conexion, index=False, index_label='id', if_exists='replace')
mi_conexion.commit()
mi_conexion.close()
mi_conexion.execute("DROP TABLE IF EXISTS precios_tb_final")
mi_conexion=get_database_connection()
table = "CREATE TABLE precios_tb_final  as SELECT precios_final.sucursal_id,precios_final.producto_id,precios_final.precio,precios_final.col_added	,precios_final.numero_archivo FROM precios_final LEFT JOIN(SELECT * FROM precios_aux) AS prec USING(sucursal_id,producto_id)"
mi_conexion.execute(table)
mi_conexion.commit()