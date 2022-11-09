select university, 
	   career, 
	   to_date(inscription_date, 'YYYY/MM/DD') as inscription_date, 
	   nombre as first_name, 
	   null as last_name, 
	   sexo as gender, 
	   birth_date as "age", 
	   null as postal_code, 
	   "location", 
	   email 
from jujuy_utn
where (university = 'universidad nacional de jujuy') and (inscription_date between '2020-09-01' and '2021-02-01');