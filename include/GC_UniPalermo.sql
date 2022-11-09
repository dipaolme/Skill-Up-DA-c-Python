select  universidad as university, 
		careers as career, 
		to_date(fecha_de_inscripcion, 'DD/MON/YY') as inscription_date, 
		names as first_name, 
		null as last_name, 
		sexo as gender, 
		birth_dates as "age", 
		codigo_postal as postal_code, 
		null as "location", 
		correos_electronicos as email
from palermo_tres_de_febrero
where universidad = '_universidad_de_palermo' and to_date(fecha_de_inscripcion, 'DD/MON/YY') between '2020-09-01' and '2021-02-01';