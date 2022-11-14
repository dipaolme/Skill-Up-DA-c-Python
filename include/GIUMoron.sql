SELECT  TO_DATE(fechaiscripccion, 'DD/MM/YYYY') AS inscription_date,
        TO_DATE(nacimiento, 'DD/MM/YYYY') AS nacimiento,
		NULL AS age, 
        universidad ,
        carrerra,
        nombrre ,
		eemail,
        sexo ,
        direccion,
		codgoposstal
		
	
FROM moron_nacional_pampa 
WHERE TO_DATE(fechaiscripccion, 'DD/MM/YYYY') BETWEEN '01-01-2019' AND '01-08-2020' ORDER BY fechaiscripccion; 