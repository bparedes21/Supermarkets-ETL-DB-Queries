DROP TABLE IF EXISTS sucursal;
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
DROP TABLE IF EXISTS producto;
CREATE TABLE producto (
    producto_id INTEGER NOT NULL PRIMARY KEY,
    marca TEXT,
    nombre TEXT,
    presentacion TEXT,
    categoria1 TEXT,
    categoria2 TEXT,
    categoria3 TEXT
);
DROP TABLE IF EXISTS precio;
CREATE TABLE precio (
    sucursal_id TEXT NOT NULL,
    producto_id INTEGER NOT NULL,
    precio REAL,
    FOREIGN KEY(sucursal_id) REFERENCES sucursal(sucursal_id),
    FOREIGN KEY(producto_id) REFERENCES sucursal(producto_id)
);
SELECT * FROM precio;

SELECT COUNT(*) FROM sucursal;

SELECT COUNT(*) FROM producto;

SELECT COUNT(*) FROM precio;

SELECT s.banderaDescripcion ,p.marca, COUNT(prec.producto_id) as cantidad_por_producto FROM precio as prec
inner join 
producto p ON p.producto_id  = prec.producto_id
inner join 
sucursal s ON s.sucursal_id  = prec.sucursal_id 
WHERE p.marca <> "SIN MARCA" and s.banderaDescripcion <>""

GROUP BY s.banderaDescripcion
ORDER BY  cantidad_por_producto DESC  
LIMIT 10;

SELECT p.marca, COUNT(prec.producto_id) as cantidad_por_producto FROM precio as prec
inner join 
producto p ON p.producto_id  = prec.producto_id
WHERE prec.precio >0 and marca <> "SIN MARCA"

GROUP BY  p.marca
ORDER BY  cantidad_por_producto DESC  
LIMIT 3