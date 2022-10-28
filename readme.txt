git remote add origin https://github.com/bparedes21/PI01_DT_ENGINEERING.git
git branch -M main
git push -u origin main

https://www.youtube.com/watch?v=hWglK8nWh60&ab_channel=Bluuweb

activate
cd..
cd..
cd jupyter
code .
python .\conexion_sql.py


.bd en un rar
DIFERENTES FORMATO DE ARCHIVO PASO A CSV 

*************
PARQUET
**************************************************************************
Nombre de archivo: producto.parquet
Nuevo nombre de archivo:producto_parquet_csv.csv
Pongo todas las letras en minuscula y si hay espacios vacios al principio o al final los quito
 Encuentro registros con contenido None (vacios) pero los dejo ya que con una consulta en sql podria filtrarlos y 
en 4 registros de las categorias hay datos.
 
*************
CSV
**************************************************************************
Nombre de archivo: sucursal.csv
Nuevo nombre de archivo:sucursal_csv.csv
  Pongo todas las letras en minuscula y si hay espacios vacios al principio o al final los quito
  No hay datos faltantes en las columnas
*************
JSON 
***************************************************************************
Nombre de archivo: precios_semana_20200503.json 
Nuevo nombre de archivo: precios_semana_20200503_csv.csv
  
Cambio el orden de las columnas para que todos los archivos tengan el mismo orden de columnas
  chequeo que no haya nulos: no encuentro registros nan (sin valor o vacios)
  transformo por columnas y agrego la columna col_added:col_added. Contiene 0(ceros).
  *Creo una nueva columna con el numero de archivo(4)
*************
CSV
***************************************************************************
Nombre de archivo: precios_semana_20200413.csv
Nuevo nombre de archivo: precios_semana_20200413_csv.csv

Cambio el orden de las columnas para que todos los archivos tengan el mismo orden de columnas
  
  transformo por columnas y agrego la columna col_added:col_added. Contiene 0(ceros).
precio:
     *si es un registro nan (sin precio) no se borra sino que se pone un 1 en la columna col_added
producto_id:
     *Creo una variable binaria para los registros en nan (sin id )o que no coincide cuando se compara el mismo con el id de la tabla sucursal:  1 si no coincide y en 0 ok
     
sucursal_id:
     *compruebo los id de banderaId-comercioId-sucursalId comparandolos con los id de la tabla sucursal
     1- 2020-04-13 *Creo una nueva columna con el numero de archivo(1)
*************
XLSX (2 hojas)
***************************************************************************
Nombre de archivo: precios_semanas_20200419_20200426.xlsx  
Separo por hojas:
La primera hoja (hoja 0) la nombro precios_semana_20200426_csv.csv 
hoja 0 ---->>>Nuevo nombre de archivo: precios_semana_20200426_csv.csv 
Cambio el orden de las columnas para que todos los archivos tengan el mismo orden de columnas
transformo por columnas y agrego la columna col_added:

precio:	
     *si es un registro nan (sin precio) no se borra sino que se pone un 1 en la columna col_added
producto_id:
     *saco el punto decimal(ej: .0)
     *Creo una variable binaria para los registros en nan (sin id )o que no coincide cuando se compara el mismo con el id de la tabla sucursal:  1 si no coincide y en 0 ok
     *Cambio el valor nan o NaN por 13 ceros (0000000000000)
    
sucursal_id:
     *compruebo los id de banderaId-comercioId-sucursalId comparandolos con los id de la tabla sucursal
     *Creo una nueva columna con el numero de archivo(3)

Cambio el orden de las columnas para que todos los archivos tengan el mismo orden de columnas
hoja 1 --->>>Nuevo nombre de archivo:  precios_semana_20200419_csv.csv
precio:	
     *si es un registro nan (sin precio) no se borra sino que se pone un 1 en la columna col_added
producto_id:
     *saco el punto decimal(ej: .0)
     *Creo una variable binaria para los registros en nan (sin id )o que no coincide cuando se compara el mismo con el id de la tabla sucursal:  1 si no coincide y en 0 ok
     *Cambio el valor nan o NaN por 13 ceros (0000000000000)
     

     *Creo una nueva columna con el numero de archivo(2)

sucursal_id:
*************
TXT 
***************************************************************************
Nombre de archivo: precios_semana_20200518.txt 
Nuevo nombre de archivo: precios_semana_20200518_csv.csv
  
Cambio el orden de las columnas para que todos los archivos tengan el mismo orden de columnas
  transformo por columnas y agrego la columna col_added:col_added. Contiene 0(ceros).
precio:
     *si es un registro nan (sin precio) no se borra sino que se pone un 1 en la columna col_added
producto_id:
     *Creo una variable binaria para los registros en nan (sin id )o que no coincide cuando se compara el mismo con el id de la tabla sucursal:  1 si no coincide y en 0 ok
     
sucursal_id:
     *compruebo los id de banderaId-comercioId-sucursalId comparandolos con los id de la tabla sucursal

     *Creo una nueva columna con el numero de archivo(5)

******************************************************************************
CREO LAS TABLAS: sucursales, clientes, precios
******************************************************************************
orden de insercion de datos segun fecha indicada en el nombre del archivo en tabla precios
1- 2020-04-13 
2- 2020-04-19 
3- 2020-04-26 
4- 2020-05-03 
5- 2020-05-18 


			
