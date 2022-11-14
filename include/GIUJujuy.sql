SELECT    TO_DATE(inscription_date,'YY/MM/DD')AS inscription_date, 
          TO_DATE(birth_date,'YY/MM/DD')as birth_date,
		  NULL AS age ,
          university, 
		  career  ,
		  nombre  ,
		  email, 
		  sexo , 
		  "location",
		  NULL AS postal_code
FROM jujuy_utn

WHERE (TO_DATE(inscription_date, 'YY/MM/DD')  BETWEEN '2019-01-01' AND '2020-08-01');


  
	

	
	
