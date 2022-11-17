<<<<<<< HEAD
SELECT 
univiersities as university,
carrera as career,
inscription_dates as inscription_date,
names as first_name,
null as last_name,
sexo as gender,
fechas_nacimiento as "age",
null as postal_code,
localidad as "location",
email
from rio_cuarto_interamericana where univiersities = 'Universidad-nacional-de-río-cuarto' and 
=======
SELECT 
univiersities as university,
carrera as career,
inscription_dates as inscription_date,
names as first_name,
null as last_name,
sexo as gender,
fechas_nacimiento as "age",
null as postal_code,
localidad as "location",
email
from rio_cuarto_interamericana where univiersities = 'Universidad-nacional-de-río-cuarto' and 
>>>>>>> cd481ad119f5296390759ecda4e6d99915d9938f
(to_date(inscription_dates, 'YY/MON/DD') between '2020-09-01' and '2021-02-01');