

/*  Create una tabla e insertar valores manualmente */



CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department TEXT NOT NULL,
    salary REAL NOT NULL
);



INSERT INTO employees (name, department, salary) VALUES ('John Doe', 'IT', 55000);
INSERT INTO employees (name, department, salary) VALUES ('Jane Smith', 'HR', 65000);


SELECT * FROM employees;


/* 
Para crear una tabla partiendo de archivos en formato csv  ejecutar en el terminal:

sqlite3 testdatabase.db
.mode csv
.import path_del_archivo nombre_tabla  //ejemplo: .import /Users/rodrigo/Downloads/employees.csv employees

Para ver las tablas creadas: 
.tables

*/




SELECT * FROM machines 

SELECT * FROM failures


/*  Uso de la sentencia where */


SELECT * FROM machines 
WHERE model = 'model3'


SELECT datetime, failure FROM failures 
WHERE failure = 'comp2'


/*  Uso de la sentencia group by */


select model, avg(age) 
from machines 
group by model


/*  El ejemplo inferior retorna resultados incorrectos porque el tipo de datos es  char */
select model, avg(age), count(1), max(age), min(age) 
from machines 
group by model


/*  esto se puede resolver utilizando la funcion cast */

select model, avg(age), count(1), max(age), min(age) 
from (select model, cast(age AS INTEGER) as age from machines) a
group by model

select model, cast(age AS INTEGER) as age from machines



/*  Ejemplos the group by */

SELECT DISTINCT failure FROM failures 



SELECT machineID, count(1) as  fallas
FROM failures 
GROUP BY machineID
ORDER BY fallas DESC

SELECT failure, count(1) as  fallas
FROM failures 
GROUP BY failure
ORDER BY fallas DESC


SELECT machineID, failure, count(1) as  fallas
FROM failures 
GROUP BY failure, machineID
ORDER BY machineID, fallas DESC



/*   */