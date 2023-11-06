

<a href="https://github.com/404"><img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%"></a>

<div align = "center">
<a href=""><img src="/Presentación/cloud_super.png" width="100%"></a>

# [Proyecto Data Engineer🧰 ETL, creacion de DB en SQLITE](#)
  
<a href="https://github.com/404"><img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%"></a>

## Table of Contents

</br>

| | Table Of Contents |
|--|----------------|
| 1 | [About](#About)  |
| 2 | [Setup](#setup)  | 
| 3 | [Consultas SQL](#ConsultasSQL)  | 




</div>

<a href="https://github.com/404"><img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%"></a>

## Directory Structure

```js
├── .py-jupyter
│   ├── jupyter
│   │──extraccion_de_archivos.py
│   │──funciones_transformacion.py
│   │── carga_de_datos.py
│   │──mi_base_de_datos.db
│   
│ 
└── README.md
```

<a href="https://github.com/404"><img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%"></a>
  
******************************************************************************

<div align = "center">

## About
Extraer y transformar archivos:
</div>
</br>

## :hammer: Formatos de archivos

- `PARQUET`
- `JSON`
- `XLSX`
- `TXT`
- `CSV`

******************************************************************************

## :factory: Transformar y cargar

- `Crear base de datos SQL llamada: mi_base_de_datos`
- `Crear tablas: precio, producto, sucursal`
- `Insertar datos con carga incremental`
- `Cargando solo los registros nuevos o medificados`

<div align = "center">

******************************************************************************

<a href=""><img src="/Presentación/Slide2.jpg" width="100%"></a>

******************************************************************************

<a href=""><img src="/Presentación/Slide1.jpg" width="100%"></a>

******************************************************************************
</div>
<a href="https://github.com/404"><img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%"></a>

## Setup

-Configuracion. Lista de librerias utilizadas

```js      

!pip install pandas
!pip install sqlite3

```


<a href="https://github.com/404"><img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%"></a>

## Consultas SQL
Se desea saber el total de productos por marca

    
```
SELECT p.marca, COUNT(prec.producto_id) as cantidad_por_producto FROM precio as prec
inner join 
producto p ON p.producto_id  = prec.producto_id
WHERE prec.precio >0 and marca <> "SIN MARCA"

GROUP BY  p.marca
ORDER BY  cantidad_por_producto DESC  
LIMIT 3

```

| marca | cantidad_por_producto |
| --- | --- |
| LOREAL     | 16028|
| ARCOR     | 14937|
| PANTENE     | 14482|


<a href="https://github.com/404"><img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%"></a>



    


			
