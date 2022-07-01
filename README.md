<div id="top"></div>
<!-- PROJECT LOGO -->
<br />
<div>
  <h1 align="center">Modelo Direccionador Fija</h1> 
  </p>
</div>


<!-- Indice -->
<details>
  <summary>Indice</summary>
  <ol>
    <li>
      <a href="#resumen">Resumen</a>
      <ul>
        <li><a href="#objetivo">Objetivo</a></li>
      </ul>
    </li>
    <li>
      <a href="#metodologia">Metodología</a>
      <ul>
        <li><a href="#target">Definición del Target</a></li>
        <li><a href="#dataset">Elaboración del Dataset</a></li>
        <li><a href="#fuentes">Fuentes</a></li>
        <li><a href="#modelo">Modelamiento</a></li>
      </ul>
    </li>
    <li><a href="#replica">Réplica</a></li> 
    <ul>
        <li><a href="#entorno">Configuración de entorno</a></li>
        <li><a href="#validacion-fuentes">Validación de fuentes</a></li>
        <li><a href="#ejecutar-replica">Ejecutar réplica</a></li>
        <li><a href="#resultados">Resultados y validaciones</a></li>
        <li><a href="#correo">Correo</a></li>
    </ul>
  </ol>
</details>

<div id="resumen"></div>

## 1. Resumen

Modelo predictivo que busca optimizar la ruta de los promotores de venta mediante mapas y una planificación inteligente.
### 1.1. Objetivo
Aumentar las altas Fija de los promotores de venta y encontrar nuevas zonas de alta demanda.
### 1.2. Justificación
La cantidad de altas Fija ha disminuido
### 1.3. Alcance
Elementos de red: HFC (trobas) y FTTH (divicaus). Ya no se vende la tecnología ADSL.
### 1.4. Usuarios internos

|Area| Integrante| Correo |
|---|---|---|
|Marketing|Fanny Zorrilla|
|Marketing|Ericka|
|Ventas|Juan Miguel|

### 1.5. Responsables

|Area| Integrante| Correo |
|---|---|---|
|Análitica Avanzada|Jonathan Raul Lopez Ramirez	|jonathan.lopezr@telefonica.com
|Análitica Avanzada|David Garcia Auccasi|david.garciaa@telefonica.com
<p align="right">(<a href="#top">inicio</a>)</p>

### 1.6. Presentación (slides) 

<div id="metodologia"></div>

## 2. Metodología

<div id="target"></div>

### 2.1. Definición del Target
En el requerimiento no se presisaba cuándo un elemento de red es un buen lugar para ir a vender. Entonces se difinió la siguiente lógica.
Se creó una matriz de 10x10 donde:
  El eje "x" era el número de registros de ventas en deciles.
  El eje "y" era el rato de convertibilidad(N° Altas/N° Ventas) en deciles.
Llegando a la conclusión de que un elemento de red era un buen lugar para ir a vender si:
  Target:1 = Elementos de red con más de """"""" registros de ventas y una convertibilidad mayor a """"" (xx%)
  Target:0 = Casos Contrarios (xx%)
  
Fuente : Planta Total de la fija para clientes con servicio de internet

Responsable de la definición: Analítica Avanzada, Marketing y Ventas.

<div id="dataset"></div>

### 2.2. Elaboración de DataSet
Especificar la forma en cómo se construyó el dataset: periodos usados, tipos de fuentes (externas, internas)
Desarrollo: Nov21, Ene22, Feb22
Validación: Mar22
Back test: Abr22
Solo se usaron fuente internas como:
  Planta Fija
  Devueltas (Comerciales, Técnicas)
  Elementos Bloqueados
  NPNF
  Segmentación (Black, Oro, Plata)
  Tráfico Voz Fija
  Cobertura Competencia (Claro, Entel, Bitel, etc.)

<div id="fuentes"></div>

### 2.3. Fuentes 
Detalle de fuentes usadas para la construcción del dataset:

| Fuente | Nombre tabla | Base de datos | Responsable | Recurrencia | Actualización |
| ------ | ----------- | ----------- | ----------- | ----------- | ----------- |
| Planta Fija | DBI_PUBLIC.BI_DWH_MARKETSHARE | Teradata | Jordy Reateguí | Mensual | 12 cada mes |
| Devueltas | DBI_PUBLIC.BI_DWH_MARKETSHARE | Teradata | Jordy Reateguí | Mensual | 12 cada mes |
| NPNF | DBI_PUBLIC.BI_DWH_MARKETSHARE | Teradata | Jordy Reateguí | Mensual | 12 cada mes |
| Segmentación | DBI_PUBLIC.BI_DWH_MARKETSHARE | Teradata | Jordy Reateguí | Mensual | 12 cada mes |
| Tráfco de Voz Fija | DBI_PUBLIC.BI_DWH_MARKETSHARE | Teradata | Jordy Reateguí | Mensual | 12 cada mes | 

<div id="modelo"></div>

### 2.3. Modelamiento

Selección de variables:
  - Importancia de variable
  - Correlación

Algoritmo : xgboost

Train(80%) y Test (20%)

Métricas en train y test: Lift = 5 y AUC = 0.86

Métricas en validación: Lift =  y AUC = 

<div id="replica"></div>

## 3. Réplica

<div id="entorno"></div>

### 3.1. Configuración de entorno de ejecución
Describir las caraterísticas del ambiente de ejecución: (laptop, cluster). De ser necesario especificar librerías a instalar.

<div id="validacion-fuentes"></div>

### 3.2. Validación de fuentes
Describir cómo validar que las fuentes estén actualizadas antes de ejecutar la réplica del modelo.

<div id="ejecutar-replica"></div>

### 3.3. Pasos para ejecutar la réplica

Detallar el paso a paso para la réplica del modelo. Especificar ruta del objeto, scripts, shells, notebooks, etc.

<p align="right">(<a href="#top">inicio</a>)</p>

<div id="resultados"></div>

### 3.4. Resultados y Validaciones

¿Cual es el resultado esperado luego de la réplica? ¿Cómo se valida?

<p align="right">(<a href="#top">inicio</a>)</p>

<div id="correo"></div>

### 3.4. Envío de correo

Detalle del correo con los resultados de la réplica y destinatarios.
