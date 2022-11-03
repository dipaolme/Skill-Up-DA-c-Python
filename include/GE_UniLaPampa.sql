SELECT universidad AS university, carrerra AS career, 
TO_DATE(fechaiscripccion, 'DD/MM/YYYY') AS inscription_date, 
NULL AS first_name,
nombrre AS last_name,
sexo AS gender, 
nacimiento AS fecha_nacimiento,
NULL AS age,
codgoposstal AS postal_code, 
NULL AS "location",
eemail AS email

FROM public.moron_nacional_pampa
WHERE universidad = 'Universidad nacional de la pampa' AND 
(TO_DATE(fechaiscripccion, 'DD/MM/YYYY') BETWEEN '2020-09-01' AND '2021-02-01')
