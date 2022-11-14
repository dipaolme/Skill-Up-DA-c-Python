SELECT universidades AS university, carreras AS career,
TO_DATE(fechas_de_inscripcion,'YY-MON-DD') AS inscription_date,
NULL AS first_name,
nombres AS last_name,
sexo AS gender,
fechas_nacimiento AS fechas_nacimiento,
NULL AS age,
codigos_postales AS postal_code,
direcciones AS "location",
emails AS email

FROM public.uba_kenedy
WHERE (universidades='universidad-de-buenos-aires' AND TO_DATE('2021-02-21','YYYY-MM-DD') >= TO_DATE(fechas_de_inscripcion,'YY-MON-DD') and 
TO_DATE(fechas_de_inscripcion,'YY-MON-DD') >= TO_DATE('2020-09-01','YYYY-MM-DD'))