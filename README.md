# Skill-Up-DA-c-Python

Repositorio del proyecto grupal a entregar que se llevo a cabo durante la aceleracion de Alkemy durante Noviembre 2022  

## Grupo 1 - AstroData

## Integrantes y grupos asignados

- [Alfredo Rodríguez](https://github.com/elalfredoignacio) - Grupo E
- [Breyner Ocampo Cardenas](https://github.com/BROC95) - Grupo B
- [Di Paola, Matias](https://github.com/dipaolme) - Grupo A
- [Francisco Ezequiel Cambiagno](https://github.com/FranciscoCambiagno) - Grupo C
- [Juan Santiago Nicotra](https://github.com/slash-w) - Grupo F
- [Wilmar Murillo Carmona](https://github.com/murillowilmar1) - Grupo I

## Objetivo

- Implementar un flujo de ejecución que ejecute un proceso ETL (extraccion, transformacion, carga) utilizando datos de Universidades Argentinas
- Realizar comparaciones y generar KPIs de los datos previamente transformados


## Contexto
Client: Ministerio de Educación de la Nación
Situación inicial

Somos un equipo de desarrollo y data analytics, que trabajamos para la consultora “MyData”
y nuestro líder técnico nos comparte un pedido comercial directamente del Consejo Nacional
de Calidad de la Educación (por sus siglas, CNCE).
El CNCE es un grupo deliberante que pertenece al Ministerio de Educación de la Nación
Argentina. 
Este se encuentra analizando opciones universitarias disponibles en los últimos 10
años para comparar datos extraídos de universidades de todo el país, públicas y privadas,
con el fin de tener una muestra representativa que facilite el análisis.
Para esto, compartieron a “MyData” información disponible de más de 15 universidades y
centros educativos con gran volumen de datos sensibles sobre las inscripciones de alumnos.
El CNCE requiere que preparemos el set de datos para que puedan analizar la información
relevante y tomar directrices en cuanto a qué carreras universitarias requieren programa de
becas, qué planes de estudios tienen adhesión, entre otros.

## Requerimientos 

- El Ministerio necesita que ordenemos los datos para obtener un archivo con sólo la
información necesaria de cierto periodo de tiempo y de determinados lugares
geográficos de una base de datos SQL (las especificaciones serán vistas en la primera
reunión de equipo). Será necesario generar un diagrama de base de datos para que se
comprenda la estructura.
- Los datos deben ser procesados de manera que se puedan ejecutar consultas a dos
universidades del total disponible para hacer análisis parciales. Para esto será
necesario realizar DAGs con Airflow que permitan procesar datos con Python y
consultas SQL.
- Calcular, evaluar y ajustar formatos de determinados datos como fechas, nombres,
códigos postales según requerimientos normalizados que se especifican para cada
grupo de universidades, utilizando Pandas.
