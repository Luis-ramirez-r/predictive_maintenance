
CREATE TABLE failures (
  timestamp DATETIME,
  machineID INTEGER,
  failure TEXT
);


-- .import "/workspaces/predictive_maintenance/data/raw/PdM_failures.csv" failures


CREATE TABLE machines (
  machineID INTEGER,
  model TEXT,
  age INTEGER
);

-- .import "/workspaces/predictive_maintenance/data/raw/PdM_machines.csv" machines


SELECT * FROM machines LIMIT 10; 

CREATE TABLE telemetry (
 timestamp DATETIME,
 machineID INTEGER,
 volt REAL,
 rotate REAL,
 pressure REAL,
 vibration REAL
);


-- .import "/workspaces/predictive_maintenance/data/raw/PdM_telemetry.csv" telemetry


SELECT machineID, AVG(volt) FROM telemetry 
GROUP BY machineID
Order BY machineID DESC


-- Join implicito

SELECT t.machineID, t.volt, m.model 
FROM  telemetry as t, machines AS m
WHERE t.machineID = m.machineID AND m.model = 'model1'



-- join the tables failures and machines 

SELECT * FROM failures
JOIN machines
ON failures.machineID = machines.machineID

-- Join y subquery 

SELECT model,failure, count(1) as n FROM (SELECT * FROM failures
JOIN machines
ON failures.machineID = machines.machineID
) GROUP BY model, failure 
ORDER BY n DESC


CREATE TABLE failure_count2 AS 
SELECT model,failure, count(1) as n FROM (SELECT * FROM failures
JOIN machines
ON failures.machineID = machines.machineID
) GROUP BY model, failure 
ORDER BY n DESC


--- Transactions 



SELECT * FROM machines LIMIT 10; 

UPDATE machines SET age =  1  WHERE model = 'model3';



SELECT * FROM machines LIMIT 10; 


BEGIN TRANSACTION;
UPDATE machines SET age =  2 WHERE model = 'model3';
UPDATE machines SET age =  5 WHERE model = 'model4';
COMMIT;


SELECT * FROM machines LIMIT 10; 


BEGIN TRANSACTION;
UPDATE machines SET age =  1 WHERE model = 'model3';
UPDATE machines SET age =  9 WHERE model = 'model4';
ROLLBACK;


SELECT * FROM machines LIMIT 10; 
