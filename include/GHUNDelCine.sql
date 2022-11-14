SELECT universities AS university, careers AS career,
TO_DATE(inscription_dates, 'DD-MM-YYYY') AS inscription_date,
NULL AS first_name,
names AS last_name,
sexo AS gender,

birth_dates AS birth_dates,
NULL AS age,
locations AS "location",
NULL AS postal_code,
emails AS email

FROM public.lat_sociales_cine
WHERE (universities='UNIVERSIDAD-DEL-CINE' AND TO_DATE('2021-02-21','YYYY-MM-DD') >= TO_DATE(inscription_dates,'DD-MM-YYYY') and 
TO_DATE(inscription_dates,'DD-MM-YYYY') >= TO_DATE('2020-09-01','YYYY-MM-DD'))