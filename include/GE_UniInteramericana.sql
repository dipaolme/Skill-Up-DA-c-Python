SELECT univiersities AS university, carrera AS career, 
TO_DATE(inscription_dates, 'YY/Mon/DD') AS inscription_date, 
NULL AS first_name,
"names" AS last_name,
sexo AS gender, 
fechas_nacimiento AS fecha_nacimiento,
NULL AS age,
NULL AS postal_code,
localidad AS "location", 
email

FROM public.rio_cuarto_interamericana
WHERE univiersities = '-universidad-abierta-interamericana' AND 
(TO_DATE(inscription_dates, 'YY/Mon/DD') BETWEEN '2020-09-01' AND '2021-02-01');