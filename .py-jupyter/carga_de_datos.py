import pandas as pd

import extraccion_de_archivos
import funciones_transformacion

sucursal_df,productos_df,precios_semana_20200413_df,precios_20200419_20200419_df,precios_20200426_20200426_df,precios_semana_20200503_df,precios_semana_20200518_df=funciones_transformacion.transformar_todos_los_archivos()

import sqlite3
def conectar_db():
    # Conectar a la base de datos (si no existe, se creará)
    conn = sqlite3.connect('mi_base_de_datos.db')
    return conn

conn=conectar_db()
cursor = conn.cursor()


# Comando SQL para crear una tabla llamada 'sucursal' con dos columnas de IDs y una columna de precios
cursor.execute('''DROP TABLE IF EXISTS sucursal;''')
create_table_query = '''
CREATE TABLE sucursal (
    sucursal_id TEXT NOT NULL PRIMARY KEY,
    comercioId INTEGER NOT NULL,
	banderaId INTEGER NOT NULL,
    banderaDescripcion TEXT,
    comercioRazonSocial	TEXT,
    provincia TEXT,
    localidad TEXT
    direccion TEXT
    lat	TEXT
    lng	TEXT
    sucursalNombre TEXT
    sucursalTipo TEXT
);
'''
cursor.execute(create_table_query)
conn.commit()
print("Tabla 'sucursal' creada exitosamente.")

#borrar tabla precio si existe
cursor.execute('''DROP TABLE IF EXISTS producto;''')

# Comando SQL para crear una tabla llamada 'producto' con dos columnas de IDs y una columna de precios
create_table_query = '''
CREATE TABLE producto (
    producto_id INTEGER NOT NULL PRIMARY KEY,
    marca TEXT,
    nombre TEXT,
    presentacion TEXT,
    categoria1 TEXT,
    categoria2 TEXT,
    categoria3 TEXT
);
'''

# Ejecutar el comando SQL para crear la tabla
cursor.execute(create_table_query)
conn.commit()
# Guardar los cambios y cerrar la conexión
print("Tabla 'producto' creada exitosamente.")

#borrar tabla precio si existe
cursor.execute('''DROP TABLE IF EXISTS precio;''')

# Comando SQL para crear una tabla llamada 'precio' con dos columnas de IDs y una columna de precios
create_table_query = '''
CREATE TABLE precio (
    sucursal_id TEXT NOT NULL,
    producto_id INTEGER NOT NULL,
    precio REAL,
    FOREIGN KEY(sucursal_id) REFERENCES sucursal(sucursal_id),
    FOREIGN KEY(producto_id) REFERENCES sucursal(producto_id)
);
'''
cursor.execute(create_table_query)
conn.commit()
print("Tabla 'precio' creada exitosamente.")


# Insertar o actualizar los datos en la tabla precio
sucursal_df.to_sql('sucursal', conn, if_exists='replace', index=False, index_label='sucursal_id')

# Guardar los cambios 
conn.commit()

# Insertar o actualizar los datos en la tabla precio
productos_df.to_sql('producto', conn, if_exists='replace', index=False, index_label='sucursal_id')

# Guardar los cambios 
conn.commit()

"""
inserto o actualizo los registros en la tabla precio los siguintes archivos:
precios_semana_20200413_df,
precios_20200419_20200419_df,
precios_20200426_20200426_df,
precios_semana_20200503_df,
precios_semana_20200518_df
"""
#####PRIMER ARCHIVO
# Insertar o actualizar los datos en la tabla precio
precios_semana_20200413_df.to_sql('precio', conn, if_exists='replace', index=False, index_label='sucursal_id')

# Guardar los cambios 
conn.commit()

#unir los archivos y borrar el primer registro de los registro duplicados.
precios_actualizados=pd.concat([precios_semana_20200413_df,precios_20200419_20200419_df]).drop_duplicates(subset =[ 'sucursal_id','producto_id'],keep='first').reset_index(drop=True)

#####SEGUNDO ARCHIVO
# Insertar o actualizar los datos en la tabla precio
precios_actualizados.to_sql('precio', conn, if_exists='replace', index=False, index_label='sucursal_id')

# Guardar los cambios 
conn.commit()

#####TERCER ARCHIVO
#consula selecciono todo de la tabla precio
query = 'SELECT * FROM precio'
#almaceno en df
df_precios_antiguos = pd.read_sql(query, conn)
#creo el dataframe precios_actualizados
precios_actualizados=pd.DataFrame()
#borro los primeros registros duplicados
precios_actualizados=pd.concat([df_precios_antiguos,precios_20200426_20200426_df]).drop_duplicates(subset =[ 'sucursal_id','producto_id'],keep='first').reset_index(drop=True)
# Insertar o actualizar los datos en la tabla precio
precios_actualizados.to_sql('precio',  conn, if_exists='replace', index=False, index_label='sucursal_id')

# Guardar los cambios 
conn.commit()

#####CUARTO ARCHIVO
#consula selecciono todo de la tabla precio
query = 'SELECT * FROM precio'
#almaceno en df
df_precios_antiguos = pd.read_sql(query, conn)
#creo el dataframe precios_actualizados
precios_actualizados=pd.DataFrame()
#borro los primeros registros duplicados
precios_actualizados=pd.concat([df_precios_antiguos,precios_semana_20200503_df]).drop_duplicates(subset =[ 'sucursal_id','producto_id'],keep='first').reset_index(drop=True)
# Insertar o actualizar los datos en la tabla precio
precios_actualizados.to_sql('precio',  conn, if_exists='replace', index=False, index_label='sucursal_id')

# Guardar los cambios 
conn.commit()

#####QUINTO ARCHIVO
#consula selecciono todo de la tabla precio
query = 'SELECT * FROM precio'
#almaceno en df
df_precios_antiguos = pd.read_sql(query, conn)
#creo el dataframe precios_actualizados
precios_actualizados=pd.DataFrame()
#borro los primeros registros duplicados
precios_actualizados=pd.concat([df_precios_antiguos,precios_semana_20200518_df]).drop_duplicates(subset =[ 'sucursal_id','producto_id'],keep='first').reset_index(drop=True)
# Insertar o actualizar los datos en la tabla precio
precios_actualizados.to_sql('precio',  conn, if_exists='replace', index=False, index_label='sucursal_id')

# Guardar los cambios 
conn.commit()
#CONSULTAR LA TABLA SUCURSAL
cursor.execute('SELECT COUNT(*) FROM sucursal')

# Obtener el resultado de la consulta
cantidad_registros = cursor.fetchone()[0]
print("En la tabla sucursal se insertaron ",cantidad_registros)

#CONSULTAR LA TABLA PRODUCTO
cursor.execute('SELECT COUNT(*) FROM producto')

# Obtener el resultado de la consulta
cantidad_registros = cursor.fetchone()[0]
print("En la tabla sucursal se insertaron ",cantidad_registros)

#CONSULTAR LA TABLA PRECIO
cursor.execute('SELECT COUNT(*) FROM precio')

# Obtener el resultado de la consulta
cantidad_registros = cursor.fetchone()[0]
print("En la tabla precio se insertaron registros totales ",cantidad_registros)

cursor.close()
conn.close()