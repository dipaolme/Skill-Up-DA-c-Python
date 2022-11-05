select universidad , carrera , fecha_de_inscripcion,nombre , sexo ,fecha_nacimiento,localidad ,email  from salvador_villa_maria svm   where  (universidad =  'UNIVERSIDAD_DEL_SALVADOR' and (to_date(fecha_de_inscripcion,'DD-Mon-YY') between '2020-09-01' and '2021-02-01')) order  by  to_date(fecha_de_inscripcion,'DD-Mon-YY')  asc;
 
