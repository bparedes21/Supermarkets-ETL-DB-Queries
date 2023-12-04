<a href="https://github.com/404"><img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="30%"></a>

<div align = "center">
	
<a href="https://github.com/bparedes21/PI01_DT_ENGINEERING/blob/main/.py-jupyter/JUPYTER/cloud.ipynb"><img src="/Presentación/cloud_super.png" width="50%"></a>

Esta imagen fue generada con Python por mi :cloud:

# [Proyecto Data Engineer🧰 ETL, creacion de DB en SQLITE](#)
  
<a href="https://github.com/404"><img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%"></a>

## Table of Contents

</br>

| | Table Of Contents |
|--|----------------|
| 1 | [About](#About)  |
| 2 | [Setup](#setup)  | 
| 3 | [Consultas_SQL](#Consultas_SQL)  | 

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

<div>

## About

### Acerca del Proyecto

Este proyecto tiene como objetivo principal la extracción, transformación y carga (ETL) de datos provenientes de diversos formatos de archivos almacenados en una carpeta de Google Drive. La ETL se realiza para varios formatos, incluyendo PARQUET, JSON, XLSX, TXT y CSV.

## Características Principales

### Proceso de ETL

1. **Extracción:**
   - Los archivos se extraen de la carpeta especificada en Google Drive.

2. **Transformación:**
   - Los datos se transforman según las necesidades del proyecto.

3. **Carga:**
   - Se crea una base de datos SQL llamada "mi_base_de_datos".
   - Se definen y crean tablas, incluyendo "precio", "producto" y "sucursal".
   - La carga se realiza de manera incremental, insertando solo registros nuevos o modificados..
   - 
### Formatos de Archivos Soportados

- **PARQUET:** Un formato eficiente para el almacenamiento de datos columnares.
- **JSON:** Un formato flexible y ampliamente utilizado para intercambio de datos.
- **XLSX:** Formato de hoja de cálculo de Microsoft Excel.
- **TXT:** Archivos de texto plano.
- **CSV:** Valores separados por comas, un formato común para datos tabulares.

<a href="https://github.com/404"><img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%"></a>

## Uso del Proyecto :factory: 

1. **Ejecución del Script:**
   ```bash
   python etl_script.py

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

## Consultas_SQL
Se desea saber el top 3 de marcas con mas productos en listados de precios del total de las sucursales. Las que mas productos tienen con precio y marca.
    
```
SELECT p.marca, COUNT(prec.producto_id) as cantidad_por_producto FROM precio as prec
inner join 
producto p ON p.producto_id  = prec.producto_id
WHERE prec.precio >0 and marca <> "SIN MARCA"

GROUP BY  p.marca
ORDER BY  cantidad_por_producto DESC  
LIMIT 3

```
Resultado de la consulta:
<div align = "center">

| marca | cantidad_por_producto |
| --- | --- |
| LOREAL     | 16028|
| ARCOR     | 14937|
| PANTENE     | 14482|

</div>

Se desea saber el top 10 de sucursales con mas productos en su listado con marca y cual marca es la que mas comercian.  

```
SELECT s.banderaDescripcion ,p.marca, COUNT(prec.producto_id) as cantidad_por_producto FROM precio as prec
inner join 
producto p ON p.producto_id  = prec.producto_id
inner join 
sucursal s ON s.sucursal_id  = prec.sucursal_id 
WHERE p.marca <> "SIN MARCA" and s.banderaDescripcion <>""

GROUP BY s.banderaDescripcion
ORDER BY  cantidad_por_producto DESC  
LIMIT 10

```
Resultado de la consulta:
<div align = "center">

| banderaDescripcion | marca | cantidad_por_producto |
| --- | --- | --- |
| Hipermercado Carrefour     | KINDER| 169854|
| La Anonima     | LA ANÓNIMA| 112012|
| Market     | CRISTAL| 95863|
| Walmart SuperCenter     | NIVEA| 73800|
| Vea     | PALADINI| 66656|
| COTO CICSA     | KINDER| 55972|
| Changomas     | NIVEA| 49507|
| Hipermercado Libertad     | RAMOLAC| 48863|
| Jumbo     | LA PAULINA| 47706|
| Cooperativa Obrera Limitada de Consumo y Vivienda     | KINDER| 46820|


</div>

<a href="https://github.com/404"><img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%"></a>



    


			
