<div id="top"></div>
<!-- PROJECT LOGO -->
<br />
<div>
  <h1 align="center">Titulo del Modelo</h1> 
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

Describir brevemente el modelo a desarrollar.
### 1.1. Objetivo
¿Cuál es el propósito del modelo?
### 1.2. Justificación
¿Cual es la necesidad ? ¿Que problema se busca satisfacer con el modelo?
### 1.3. Alcance
¿Cual es la población objetivo del modelo? ¿Sobre qué universo se trabajará el modelo y la réplica?
### 1.4. Usuarios internos

|Area| Integrante| Correo |
|---|---|---|
|||
|||

### 1.5. Responsables

|Area| Integrante| Correo |
|---|---|---|
|||
|||
<p align="right">(<a href="#top">inicio</a>)</p>

### 1.6. Presentación (slides) 

<div id="metodologia"></div>

## 2. Metodología

<div id="target"></div>

### 2.1. Definición del Target
¿Cómo se construyó el target? Detallar Fuentes, lógica, responsables de definiciones, etc.

<div id="dataset"></div>

### 2.2. Elaboración de DataSet
Especificar la forma en cómo se construyó el dataset: periodos usados, tipos de fuentes (externas, internas)

<div id="fuentes"></div>

### 2.3. Fuentes 
Detalle de fuentes usadas para la construcción del dataset:

| Fuente | Nombre tabla | Base de datos | Responsable | Recurrencia | Actualización |
| ------ | ----------- | ----------- | ----------- | ----------- | ----------- |
| Market Share | DBI_PUBLIC.BI_DWH_MARKETSHARE | Teradata | Jordy Reateguí | Mensual | 12 cada mes |

<div id="modelo"></div>

### 2.3. Modelamiento

Describir el proceso de modelamiento: selección de variables, elección de algoritmo, técnicas de balanceo, preprocesamiento de variables, selección de train-test, optimización de hiperparámetros, métricas de validación del modelo, validación en back test.


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