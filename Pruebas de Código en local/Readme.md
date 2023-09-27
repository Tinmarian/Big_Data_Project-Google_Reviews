## ***__Proyecto Grupal__***

## Sprint # 1

## Reseñas y Recomendaciones Google Maps - Yelp
![Alt text](./Imagenes_Readme/yelp-logo.png)

![Alt text](./Imagenes_Readme/Google_Maps.png) 


## Nombre de la Empresa



![Alt text](./Imagenes_Readme/LogoDataistas.jpeg)

## Distribución de Roles
|Integrante|Rol|
|:-----:|:---:|
|Jorge Gómez | Data Analyst|
|Federico Gravina | Data Engineer|
Tinmar Andrade | Data Engineer|
Fanny Fitz |Data Scientist
Jocelyn Romano | Data Scientist

---

## **Planteamiento del problema**

----

El problema que abordaremos en este proyecto se relaciona con la necesidad de mejorar la visibilidad y el impacto del negocio de nuestro cliente, que opera el restaurante "KFC" en el estado de Georgia en Estados Unidos. 

Actualmente, el restaurante “KFC” desconoce la opinión general que tiene el público de la calidad de sus servicios. 

Además, la influencia de las reseñas en plataformas como Yelp en las decisiones de los consumidores se ha vuelto cada vez más importante.

La falta de una estrategia sólida de gestión de la reputación en línea y la insuficiente atención a las opiniones de los clientes pueden estar afectando negativamente la percepción del restaurante y su capacidad para atraer nuevos clientes.

---

## **Alcance del proyecto**

---
El proyecto se enfocará en restaurantes de la cadena “KFC” ubicados en las ciudades de California, Nueva York, Colorado, Texas y Georgia, dentro de los Estados Unidos. 

De igual forma se incluirán los datos de la cadena Domino 's Pizza, McDonald's, Subway, Pizza Hut  y afines localizados en los estados ya mencionados. 

Identificación de oportunidades de mejora en estados más turísticos de Estados Unidos (California, New York, Colorado, Texas y Georgia)
Análisis exhaustivo de la industria gastronómica integrando las reseñas de Google Maps y Yelp
Enfoque: _Proporcionar recomendaciones informadas y estratégicas para la toma de decisiones_

---

## **Obejtivo General**

![Alt text](./Imagenes_Readme/Objetivo%20General.png)

---

Mejorar el posicionamiento global de los locales de la compañía KFC con respecto a sus competidores directos en los estados de Estados Unidos ubicados en  California, Nueva York, Colorado, Texas y Georgia. 

La selección de dichos estados se basa en un relevamiento de datos, que nos lleva a observar la representatividad de la muestra para luego pensar en un modelo exponencial al resto del país. Sus características diferentes en cuanto a ubicación geográfica, turismo recibido tanto interno como externo así como la diferencia del tipo de  población también nos permiten tener un mejor entendimiento de la problemática general, así como nos proporciona una mayor capacidad para realizar recomendaciones a nuestro cliente. 

Con el desarrollo del proyecto y el modelo, sería posible, en un futuro, ampliar los alcances a todo el país y también al resto de los países en donde se encuentre ubicada la cadena de comida. 

____

## **Objetivos Específicos**

---
Se recopilará información como reseñas y calificaciones provenientes de plataformas como Maps y Yelp.
Garantizar la disponibilidad de datos limpios y estructurados.

Se realizará un análisis de sentimiento sobre las reseñas de Google Maps y Yelp, esto con el fin de entender la percepción que tienen los clientes sobre el servicio en dichos restaurantes. 

_Con los resultados obtenidos se realizará una comparativa entre nuestro cliente KFC contra sus demás sucursales ubicadas en los estados ya mencionados_. 
Se comparará el desempeño entre las cadenas KFC vs su competencia a través de locales afines y el análisis de  información proveniente de las plataformas ya mencionadas. 

Crear una interfaz interactiva que visualice y explore los resultados del análisis.

---
## **Análisis Preliminar del Dato**
Se realizó un análisis preliminar de los dataset: 

Yelp:

-Business.plk

-Checkin.json

-Tip.json

-Review.json

-User.parquet

Google Maps:

-Metadata.json

-Reviews por estado.json

En el documento [analisis_preliminar_del_dato](https://docs.google.com/spreadsheets/d/1YwkOk_5r0JqUkhL_tp5NHLAcaPJU1YBV/edit#gid=1462331087) anexado en la carpeta “Sprint_1” en Drive podrás encontrar un análisis detallado de cada uno de los datasets ya mencionados, este incluye: 
- descripción general del archivo
- nombre de cada columna, tipo de dato de cada columna
- descripción de cada columna, cantidad de datos, tipo de archivo,cantidad de nulos.

De los datasets de Yelp, se plantea utilizar el archivo Business.plk,  ya que contiene información de los establecimientos,  review.json y tip.json porque contiene los reviews de cada establecimiento. 

De los datasets de Google Maps se utilizarán los archivos metadata.json y los archivos de las carpetas California, New York, Colorado, Texas y Georgia que contiene las review por estado.

-----

## **Análisis Stack Tecnológico**


![Alt text](./Imagenes_Readme/Stack%20Tecnologico.png)

___

## **KPI’s**

---

*KPI 1*: _Realizar un análisis exploratorio de los datos para comparar el rendimiento en cantidad de locales, así como en promedio de estrellas recibidas, calificación general de la cadena KFC, con respecto a sus principales competidores, a saber: Subway, MacDonald´s, Domino´s Pizza y Pizza Hut._


### Promedio de estrellas para Yelp

|Compañía| Promedio Estrellas|
|:--------|:----:|
Subway|   2.6
Domino´s Pizza| 2.28
Pizza hut| 2.03
Mc Donald´s | 1.86
KFC| 1.86


### Número de locales para Yelp

|Compañía| Locales|
|:--------|:--------:|
Mc Donald´s | 703
Subway | 459
Domino´s Pizza| 295
Pizza Hut | 272
KFC | 171


---

*KPI 2*: _Mejorar para un périodo de 3 meses un porcentaje significativo, la cantidad de reseñas positivas en los estados seleccionados a través de un análisis de sentimientos de los datos provenientes de las fuentes Google Maps y Yelp._

Para lograr este objetivo se realizará un proceso de Machine Learning que permita recibir nuevos datos. 

---

*KPI 3*:  _Hacer un análisis exhaustivo de los datos de las fuentes citadas para aumentar la cantidad de reseñas recibidas para la cadena KFC enfocados en recomendaciones de mejoras en los rubros que poseen malas calificaciones_(por ejemplo: calidad de las comidas, limpieza de los locales, rapidez del servicio, etc.)

### Cantidad de Reseñas

|Compañía| Locales|
|:--------|:-------:|
|McDonald's |17359|
|Domino's Pizza |6291|
|Pizza Hut| 17359|
|Subway| 4588|
|KFC| 2923|


---

## **Diagrama de Gantt para Sprint #1**

A continuación se muestra la distribución de los tiempos que se le destinaron para cada una de las tareas establecidas


---

## Repositorio Git 
![Logo Github](./Imagenes_Readme/Logo%20github.jpeg)

[Repositorio Github](https://github.com/Tinmarian/Proyecto_Grupal_HENRY/tree/main)
