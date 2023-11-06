import pandas as pd
import extraccion_de_archivos
#si es nulo cualquier valor de las 3 columnas se agrega un 1 a la columna col_added
#devuelve una columna llamada col_added que contiene un valor booleano 
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
#algunos id en producto_id tienen un 0. delante se borra y
#completa con ceros a la izquierda
def sacar_punto_decimal_de_producto_id(sheet1):
    sheet1["producto_id"]=sheet1["producto_id"].astype('str')
    
    new1= sheet1["producto_id"].str.split(".", n = 1, expand = True)
    sheet1["producto_id"]=new1[0]
    sheet1["producto_id"]= sheet1["producto_id"].str.zfill(13)
    return sheet1


def cambiar_separadores_de_sucursal_id(sucursal_list):
  
    for i, palabra in enumerate (sucursal_list):
    
        for caracter in palabra:
            if(caracter.isnumeric()==False)and(caracter!="-"):
                palabra=palabra.replace(caracter,"-")
                sucursal_list[i]=palabra
    
    return sucursal_list
                
def transformar_hoja_excel(new_df):
    new_df=columna_boolean_nan(new_df)
    new_df1=new_df[new_df["col_added"]==0]
    new_df=pd.DataFrame()
    new_df=new_df1.copy()
    new_df2=sacar_punto_decimal_de_producto_id(new_df)
    new_df=pd.DataFrame()
    new_df=new_df2.copy()
    new_df4=cortar_columna_sucursal_id_precio(new_df)
    new_df=pd.DataFrame()
    new_df=new_df4.copy()
    sucursal_list=list(new_df["sucursal_id"])

    sucursal_list=cambiar_separadores_de_sucursal_id(sucursal_list)
    new_df["sucursal_id"]=sucursal_list
    
    
    return new_df

def quitar_id_sucursal_invalidos(list_suc_unicos,list_suc_prec,df_precios):
    lis_indice=[]
    lis_ids_not_match=[]
    lista_ids_sucursal=list(df_precios["sucursal_id"])
    for e in list_suc_prec:

        result_id=1
        for indice, valor in enumerate(list_suc_unicos) :
            
            if(e == valor):
                result_id=0
                val_ind=valor
            
    if (result_id == 1):  
        #guardo los valores para los id de sucursal que no coinciden 
        lis_ids_not_match.append(e)
    
    for posicion_dato,dato_sucursal_id in enumerate(lis_ids_not_match) :

        df_precios.drop(df_precios[df_precios['sucursal_id'] ==dato_sucursal_id].index, inplace=True)
    
    return df_precios

def transformar_precios_semana_20200413_df(df):
    new_df=columna_boolean_nan(df)
    df=pd.DataFrame()
    new_df1=new_df[new_df["col_added"]==0]
    df=new_df1.copy()
    new_df2=sacar_punto_decimal_de_producto_id(df)
    df=pd.DataFrame()
    df=new_df2[["precio", "producto_id", "sucursal_id"]]
    return df
def transformar_precios_semanas_20200419_20200426_df(sucursal_df,df,hoja):
    new_dataframe=transformar_hoja_excel(df)
    df=pd.DataFrame()
    df=new_dataframe.copy() 
    if(hoja=="20200419"):

        list_suc_unicos=array_col_id_unicos(sucursal_df,"id")
        list_suc_prec=array_col_id_unicos(df,"sucursal_id")
        new_TablaPrecios=quitar_id_sucursal_invalidos(list_suc_unicos,list_suc_prec,df)
        df=pd.DataFrame()
        df=new_TablaPrecios
    return df

def transformar_precios_semana_20200503_df(df):
    new_df=sacar_punto_decimal_de_producto_id(df)
    df=pd.DataFrame()
    df=new_df.copy()
    return df
def trasformar_archivo_sucursal(df):
    list_sucursal=list(df["id"])
    list_sucursal=cambiar_separadores_de_sucursal_id(list_sucursal)
    df["id"]=list_sucursal
    return df

def transformar_precios_semana_20200518_df(df,sucursal_df):
    new_df1=columna_boolean_nan(df)
    new_df1=new_df1[new_df1["col_added"]==0]
    df=pd.DataFrame()
    df=new_df1.copy()
    list_suc_unicos=array_col_id_unicos(sucursal_df,"id")
    list_suc_prec=array_col_id_unicos(df,"sucursal_id")
    new_TablaPrecios=quitar_id_sucursal_invalidos(list_suc_unicos,list_suc_prec,df)
    df=pd.DataFrame()
    df=new_TablaPrecios.copy()
    return df
def transformar_todos_los_archivos():
    sucursal_df,productos_df,precios_semana_20200413_df,precios_20200419_20200419_df,precios_20200426_20200426_df,precios_semana_20200503_df,precios_semana_20200518_df=extraccion_de_archivos.importar_todos_los_archivos()
    sucursal_df=trasformar_archivo_sucursal(sucursal_df)
   
    #precios_semana_20200413.csv
    new_df=pd.DataFrame()
    new_df=transformar_precios_semana_20200413_df(precios_semana_20200413_df)
    precios_semana_20200413_df=pd.DataFrame()
    precios_semana_20200413_df= new_df[["sucursal_id"	,"producto_id",	"precio"]]
   
    #hoja
    #precios_20200419_20200419
    new_df=pd.DataFrame()
    hoja="20200419"
    new_df=transformar_precios_semanas_20200419_20200426_df(sucursal_df,precios_20200419_20200419_df,hoja)
    precios_20200419_20200419_df=pd.DataFrame()
    precios_20200419_20200419_df= new_df[["sucursal_id","producto_id","precio"]]

    #hoja

    #precios_20200426_20200426
    new_df=pd.DataFrame()
    hoja="20200426"
    new_df=transformar_precios_semanas_20200419_20200426_df(sucursal_df,precios_20200426_20200426_df,hoja)
    precios_20200426_20200426_df=pd.DataFrame()
    precios_20200426_20200426_df= new_df[["sucursal_id"	,"producto_id",	"precio"]]
    #precios_semana_20200503
    new_df=pd.DataFrame()
    new_df=transformar_precios_semana_20200503_df(precios_semana_20200503_df)
    precios_semana_20200503_df=pd.DataFrame()
    precios_semana_20200503_df= new_df[["sucursal_id"	,"producto_id",	"precio"]]
    #precios_semana_20200518
    new_df=pd.DataFrame()
    new_df=transformar_precios_semana_20200518_df(precios_semana_20200518_df,sucursal_df)    
    precios_semana_20200518_df=pd.DataFrame()
    precios_semana_20200518_df= new_df[["sucursal_id"	,"producto_id",	"precio"]]
    #cambiar el nombre de las columnas con las claves primarias
    sucursal_df.rename(columns={'id':'sucursal_id'},
               inplace=True)
    productos_df.rename(columns={'id':'producto_id'},
               inplace=True)

    return sucursal_df,productos_df,precios_semana_20200413_df,precios_20200419_20200419_df,precios_20200426_20200426_df,precios_semana_20200503_df,precios_semana_20200518_df