SELECT 
universidad as university,
carrerra as career,
fechaiscripccion as inscription_date,
nombrre as first_name,
null as last_name,
sexo as gender,
nacimiento as "age",
codgoposstal as postal_code,
null as "location",
eemail as email
from moron_nacional_pampa where universidad = 'Universidad de morón' and 
(to_date(fechaiscripccion, 'DD/MM/YY') between '01-09-2020' and '01-02-2021');