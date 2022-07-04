#!/usr/bin/env python
# coding: utf-8

# In[1]:


print(" ")
print("REPLICA DEL MODELO DE DIRECCIONADOR + MAPAS")


# In[2]:


import os
import sys
from multiprocessing import Pool
import pandas as pd
import numpy as np
import time
import logging
import xgboost as xgb
import glob
import subprocess
from datetime import datetime #fecha actual.
from execution_framework.utils import db_utils #funciones manipular teradata y traer info de HIVE, conexiones
from execution_framework.fastload_teradata import fastload_dataframe #llevar las tabla del cluster a teradata.
from execution_framework.utils.common_utils import read_configuration_file #leer configuraciones
from execution_framework.utils.db_utils import teradata_connection, hive_connection, read_query_to_df, execute_store_procedure #ejecucion de los sp
from dateutil.relativedelta import relativedelta
from datetime import timedelta, date, datetime

#direccionador.
import geopandas as gpd
import matplotlib.pyplot as plt
from dash import Dash, dcc, html, Output, ctx, Input
import plotly.express as px


# ## 0. Configuración

# In[4]:


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s :: %(levelname)s :: %(name)s  :: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('MAIN')

os.chdir('/home/dgarciaau/cobranza/FERBI/Direccionador/')

# Leyendo configuracion:
conf_param = read_configuration_file('./main_conf_direccionador.yaml')

#VARIABLES:
user = conf_param['teradata-credentials']['user']
password = conf_param['teradata-credentials']['pass']
t_conn = db_utils.teradata_connection(user, password)


# In[5]:


#Generacion de variables:
logger.info('1.2 Creaciones de variables globales:')
periodo_m1 = conf_param["periodo_m1"]
periodo_yyyymm = (datetime.strptime(periodo_m1, '%Y-%m-%d')).strftime("%Y%m")
periodo_yyyymmdd = (datetime.strptime(periodo_m1, '%Y-%m-%d')).strftime("%Y%m%d")


# ## 1. Leyendo Capas

# In[6]:


# Cargando las capaas que nos interesan y seleccionando algunas columnas
#DIVICAUS
logger.info('2. Proceso para la obtencion de la poblacion:')
ruta_mapa = conf_param["ruta_mapa"]

divicau = gpd.read_file(ruta_mapa, layer='LY_DIVICAU')
divicau['TIPO_ELEMENTO']='DIVICAU'
divicau = divicau.loc[:,['COD_PLA','TIPO_ELEMENTO','SHAPE_Length','SHAPE_Area','geometry']]
#TROBAS
planos = gpd.read_file(ruta_mapa, layer='LY_PLANOS')
planos['TIPO_ELEMENTO']='TROBAS'
planos = planos.loc[:,['COD_PLA','TIPO_ELEMENTO','SHAPE_Length','SHAPE_Area','geometry']]

# 2 tablas principales: divi_planos - df_geometria
divi_planos_00 = pd.concat([divicau, planos], ignore_index=True)
divi_planos_00 = divi_planos_00.rename({'COD_PLA': 'ELEMENTO', 'TIPO_ELEMENTO':'TIPO_ELEMENTO','SHAPE_Length': 'SHAPE_LENGTH', 'SHAPE_Area': 'SHAPE_AREA', 'geometry': 'GEOMETRIA'}, axis=1)
divi_planos = divi_planos_00[["ELEMENTO","TIPO_ELEMENTO"]]
df_geometria = divi_planos_00[["ELEMENTO","GEOMETRIA"]]


# In[26]:


logger.info('3. Llevando la poblacion a teradata:')
t_conn = db_utils.teradata_connection(user, password)
nombre_tabla = 'ACOB_CAPAS_TEMPORAL'
#d2 = d2.astype('string')
fastload_dataframe(t_conn, divi_planos, nombre_tabla, overwrite_table = True, primary_index = True, primary_index_cols = ['ELEMENTO'])
logger.info('Fin dl migrado')


# In[60]:


logger.info('4. Insertando historico de capas(poblacion)')
#insertando al historico
query_t0 = """
DELETE FROM ACOB_DIREC_POBLA_HISTORIA WHERE PERIODO = '{}';
INSERT INTO ACOB_DIREC_POBLA_HISTORIA
SEL
'{}' AS PERIODO,
ELEMENTO,
TIPO_ELEMENTO,
CURRENT_DATE AS EJECUCION,
CURRENT_TIMESTAMP(0) FECHA_HORA
FROM ACOB_CAPAS_TEMPORAL
"""
sql = query_t0.format(periodo_yyyymm,periodo_yyyymm)
db_utils.execute_db_statement(t_conn,sql);


# ### Ejecucion del SP

# In[65]:


logger.info('5. Ejecucion del procedure para la generacion de la matriz')
data_replica = conf_param['historical-dataset-table']['procedure-to-generate-matriz']
data_replica['parameters']['PERIODO_M1'] = periodo_yyyymm
execute_store_procedure(t_conn,data_replica)


# ## Leyendo la matriz de datos

# In[82]:


logger.info('6. Leyendo la data para replica')
query = """
SELECT * FROM ACOB_DIR_MATRIX_HISTORIA WHERE PERIODO = '{}'
"""
#setienda variables:
sql = query.format(periodo_m1)
data = db_utils.read_query_to_df(t_conn,sql)
data1 = data

#variables:
logger.info('7. Replica del modelo')
predictores = ['F1_Q_INT_VEL_70','F2_SUM_DEVUELTA_COMERCIAL_U3M','F5_Q_FACTURAS','F2_SUM_DEVUELTA_TECNICA_U3M',
'F1_Q_FTTH','F1_Q_ZONA_ORO','F1_P_FTTH','F7_AVG_tmin_3M','F1_MIN_ANTIGUEDAD_CLI']

model = xgb.XGBClassifier()
model.load_model('fit_dir_00.json')

df2 = model.predict_proba(data1[predictores].astype(float))
df2 = pd.DataFrame(df2)
data1['PROB'] = df2[1]
columnas = ['PERIODO','ELEMENTO','PROB']
data1 = data1[columnas]


# In[78]:


logger.info('8. Llevando la probabilidades a teradata.')
nombre_tabla = 'ACOB_DIREC_REPLICA_TEMP_1'
#d2 = d2.astype('string')
fastload_dataframe(t_conn, data1, nombre_tabla, overwrite_table = True, primary_index = True, primary_index_cols = ['PERIODO','ELEMENTO'])
logger.info('Fin')


# In[101]:


logger.info('9. Insertando al historico de probabilidades')
#insertando al historico
query_t1 = """
DELETE FROM ACOB_DIR_PROB_HISTORIA WHERE PERIODO = '{}';
INSERT INTO ACOB_DIR_PROB_HISTORIA
SELECT
CAST(PERIODO AS DATE) AS PERIODO,
A.ELEMENTO,
CAST(PROB AS DECIMAL(10,8)) AS PROB,
CASE
    WHEN PROB<0.1235096  THEN 'D10'
    WHEN PROB<0.1240414  THEN 'D09'
    WHEN PROB<0.1250448  THEN 'D08'
    WHEN PROB<0.127221   THEN 'D07'
    WHEN PROB<0.1400322  THEN 'D06'
    WHEN PROB<0.1507134  THEN 'D05'
    WHEN PROB<0.1758088  THEN 'D04'
    WHEN PROB<0.2163369  THEN 'D03'
    WHEN PROB<0.2882632  THEN 'D02'
    WHEN PROB>=0.2882632 THEN 'D01'
END AS DECIL
FROM ACOB_DIREC_REPLICA_TEMP_1 A;
"""
sql = query_t1.format(periodo_m1,periodo_m1)
db_utils.execute_db_statement(t_conn,sql);


# In[ ]:


## PROCESOS QUE NO SE EJECUTARÁN:
# PLANTA ESTATICAS.
# SELECT * FROM ACOB_DIR_UBIGEO (UBIGEO)


# In[114]:


logger.info('10. Generacion de data de saturacion')
ruta_saturacion = conf_param["ruta_saturacion"]

hfc = pd.read_excel (ruta_saturacion,skiprows=2)
hfc['TIPO_ELEMENTO'] = 'TROBAS'
hfc_1 = hfc.iloc[:,[1,8,10,11]]
hfc_1.columns = ['ELEMENTO','SEMANA','SATURACION','TIPO_ELEMENTO']
hfc_2 = hfc.iloc[:,[2,8,10,11]]
hfc_2.columns = ['ELEMENTO','SEMANA','SATURACION','TIPO_ELEMENTO']
hfc_3 = hfc.iloc[:,[3,8,10,11]]
hfc_3.columns = ['ELEMENTO','SEMANA','SATURACION','TIPO_ELEMENTO']
hfc = pd.concat([hfc_1, hfc_2,hfc_3], ignore_index=True)
hfc['NCHAR'] = hfc['ELEMENTO'].str.len()
hfc = hfc[hfc.ELEMENTO.notnull() & hfc.NCHAR.isin([6])]
hfc = hfc.iloc[:,[0,3,2,1]]


# In[122]:


logger.info('11. Llevando la info de saturacion trobas a teradata:')
nombre_tabla = 'ACOB_DIREC_SATURA_TEMP'
#d2 = d2.astype('string')
fastload_dataframe(t_conn, hfc, nombre_tabla, overwrite_table = True, primary_index = True, primary_index_cols = ['ELEMENTO'])
logger.info('Fin de exportación.')


# In[123]:


logger.info('12. Insert al historico de saturacion')
#insertando al historico
query_t2 = """
DELETE FROM ACOB_DIR_SATU_HISTORIA WHERE PERIODO = '{}' AND SEMANA = '{}';
INSERT INTO ACOB_DIR_SATU_HISTORIA
SELECT
'{}' AS PERIODO,
ELEMENTO,
TIPO_ELEMENTO,
SATURACION,
SEMANA,
CURRENT_DATE AS EJECUCION,
CURRENT_TIMESTAMP(0) FECHA_HORA
FROM ACOB_DIREC_SATURA_TEMP;
"""
sql = query_t2.format(periodo_yyyymm,periodo_yyyymm,conf_param["semana"])
db_utils.execute_db_statement(t_conn,sql);


# In[133]:


logger.info('13. SP para los MAPAS')
dicc_mapa = conf_param['historical-dataset-table']['procedure-to-generate-mapas']
dicc_mapa['parameters']['PERIODO_M1'] = periodo_yyyymm
execute_store_procedure(t_conn,dicc_mapa)


# ### leyendo la data para los MAPAS

# In[8]:


logger.info('### DASH PARA MAPAS ###')
logger.info('14. Leyendo la data para los mapas')
query = """
SELECT * FROM ACOB_DIR_VISUAL_HISTORIA WHERE PERIODO = '{}'
"""
#setienda variables:
sql = query.format(periodo_m1)
df = db_utils.read_query_to_df(t_conn,sql)


df_final = df_geometria.merge(df, on='ELEMENTO', how='inner')
df_final['DECIL_BI_2'] = df_final['DECIL_BI'].str.slice(1, 3).astype(int)

#df_final['GEOMETRIA'] = gpd.GeoSeries.from_wkt(df_final['GEOMETRIA'])

df = df_final
# Ordenando el DF para tener los periodos ordenados
df = df.sort_values(['PERIODO','DEPARTAMENTO','PROVINCIA','DISTRITO','TIPO_ELEMENTO','TIPO_CONVER','TIPO_SATU'])


# In[12]:


logger.info('### DASH PARA MAPAS ###')
logger.info('14. Leyendo la data para los mapas')
query = """
SELECT * FROM ACOB_DIR_VISUAL_HISTORIA WHERE PERIODO = '{}'
"""
#setienda variables:
sql = query.format(periodo_m1)
df = db_utils.read_query_to_df(t_conn,sql)


# In[7]:


df.head()


# In[13]:


df_final = df_geometria.merge(df, on='ELEMENTO', how='inner')
df_final['DECIL_BI_2'] = df_final['DECIL_BI'].str.slice(1, 3).astype(int)
#df['GEOMETRIA'] = gpd.GeoSeries.from_wkt(df['GEOMETRIA'])
df = df_final
# Ordenando el DF para tener los periodos ordenados
df = df.sort_values(['PERIODO','DEPARTAMENTO','PROVINCIA','DISTRITO','TIPO_ELEMENTO','SEMAFORO'])
df["PERIODO"] = df["PERIODO"].astype('string')

df['TECNOLOGIA']=df.loc[df['TIPO_ELEMENTO'] == 'DIVICAU','TECNOLOGIA'] = 'FTTH'
df.loc[df['TIPO_ELEMENTO'] == 'TROBAS','TECNOLOGIA'] = 'HFC'


# In[14]:


df.dtypes


# In[ ]:


P_Penetracion


# ## IMPORTANTE

# ## Dash prueba

# In[ ]:


# Inicial el Dash
app = Dash(__name__)

app.layout = html.Div([
    html.Div([
        html.Div([
            html.H3('MAPA DIRECCIONADOR',style={'margin-top': '-70px', 'color': 'white','margin-left': '-100px'}),
        ], className = 'two column', id = 'title'),
    ], id = 'header', className= 'row flex-display', style={'margin-bottom': '0px'}),
    
    html.Div([
        html.Div([
            html.Label('Periodo:', style={'color': 'white','margin-top':'-10px'}),
            dcc.Dropdown(id='id_periodo',
                         value = df["PERIODO"].max(),
                         options = [{'label':c,'value':c}
                                    for c in  df["PERIODO"].unique()]),
            
        ], className='create_container two columns', style={'height': '80px','width':'200px','margin-left':'-52px'}),
        
        html.Div([
            html.Label('Departamento:', style={'color': 'white','margin-top':'-10px'}),
            dcc.Dropdown(id='id_departamento',
                         #multi = False,
                         #searchable= True,
                         options = [],
                         className = 'dcc_compon'),
            
        ], className='create_container three columns', style={'height': '80px','width':'250px'}), #80
        
        html.Div([
            html.Label('Provincia:', style={'color': 'white','margin-top':'-10px'}),
            dcc.Dropdown(id='id_provincia',
                         multi = False,
                         searchable= True,
                         options = [],
                         className = 'dcc_compon')
            
        ], className='create_container three columns', style={'height': '80px','width':'250px'}),
        
        html.Div([
            html.Label('Distrito:', style={'color': 'white','margin-top':'-10px'}),
            dcc.Dropdown(id='id_distrito',
                         multi = True,
                         #searchable= True,
                         options = [],
                         className = 'dcc_compon')
            
        ], className='create_container three columns', style={'height': '80px','width':'600px'}),
        

        html.Div([
            html.Label('Descargar CSV:', style={'color': 'white','margin-top':'-8px'}),
            html.Button("Click aqui!", id="id_descarga_boton_csv",style={'color': 'white','margin-top':'1px'}),
            dcc.Download(id="id_descarga_csv")  
        ], className='create_container two columns',style={'height': '80px','width':'180px'}),
        
        
        html.Div([
            html.Label('Descargar EXCEL:', style={'color': 'white','margin-top':'-8px'}),
            html.Button("Click aqui!", id="id_descarga_boton_excel",style={'color': 'white','margin-top':'1px'}),
            dcc.Download(id="id_descarga_excel")
            
        ], className='create_container two columns',style={'height': '80px','width':'180px'})
        
    ], className='row flex-display', style={'margin-top': '-40px','width':'1594px'}),
    
    html.Div([
        
        html.Div([
            html.Label('Tecnologia:', style={'color': 'white','margin-top':'-10px'}),
            dcc.Checklist(id='id_tipo_elemento',
                          options = [],
                          className = 'dcc_compon',
                          inline=True,
                          labelStyle = {'color':'white'}
                         )
        ], className='create_container three colum',style={'height': '34px','width':'150px','margin-left':'-52px','margin-top':'38px'}), #35
        
        
        html.Div([
            html.Label('Deciles BI:', style={'color': 'white','margin-top':'-10px'}),
            dcc.RangeSlider(id='id_decil',
                            min = 1,
                            max = 10,
                            step = 1,
                            value=[1,10],
                            marks = {
                            1: {'label': 'D1', 'style':{'color':'#82E0AA'}},
                            2: {'label': 'D2', 'style':{'color':'#82E0AA'}},
                            3: {'label': 'D3', 'style':{'color':'#82E0AA'}},
                            4: {'label': 'D4', 'style':{'color':'#82E0AA'}},
                            5: {'label': 'D5', 'style':{'color':'#82E0AA'}},
                            6: {'label': 'D6', 'style':{'color':'red'}},
                            7: {'label': 'D7', 'style':{'color':'red'}},
                            8: {'label': 'D8', 'style':{'color':'red'}},
                            9: {'label': 'D9', 'style':{'color':'red'}},
                            10: {'label': 'D10','style':{'color':'red'}}}
                                )
        ], className='create_container', style = {'margin-left':'10px','margin-top':'38px','height': '35px','width':'1000px'}),
        
        html.Div([
            html.Label('Semaforo:', style={'color': 'white','margin-top':'-7px'}),
            dcc.Checklist(id='id_semaforo',
                          options = [],
                          className = 'dcc_compon',
                          inline=True,
                          labelStyle = {'color':'white'}
                         )

        ], className='create_container three colum',style={'height': '35px','width':'340px','margin-left':'10px','margin-top':'38px'})
        
    ], className='row flex-display', style={'margin-top': '-40px','width':'1594px'}),

    html.Div([
        dcc.Graph(id="id_mapa",config={'displayModeBar': 'hover'}, style = {'height': '500px'})
    ], className='create_container', style = {'margin-left':'-52px','margin-top':'-3px','width':'1600px'}),
    
], id = 'mainContainer', style={'display': 'flex', 'flex-direction': 'column'})

#-----------------------------  DEPARTAMENTO ----------------------------- #
@app.callback(
    Output("id_departamento", "options"),
    Input("id_periodo", "value"))
def departamento_options(periodo_value):
    df_dep = df[df['PERIODO'] == periodo_value]
    return [{'label':i, 'value':i} for i in df_dep['DEPARTAMENTO'].unique()]

@app.callback(
    Output("id_departamento", "value"), 
    Input("id_departamento", "options"))
def departamento_values(dep_opt):
    return [k['value'] for k in dep_opt][0]

#-----------------------------  PROVINCIA ----------------------------- #
@app.callback(
    Output("id_provincia", "options"), 
    Input("id_periodo", "value"),
    Input("id_departamento", "value"))
def provincia_options(periodo_value,departamento_value):
    df_prov = df[df['PERIODO'] == periodo_value]
    df_prov = df_prov[df_prov['DEPARTAMENTO'] == departamento_value]
    return [{'label':i, 'value':i} for i in df_prov['PROVINCIA'].unique()]
# Valor inicial de la provincia
@app.callback(
    Output("id_provincia", "value"), 
    Input("id_provincia", "options"))
def provincia_values(prov_opt):
    return [k['value'] for k in prov_opt][0]

#-----------------------------  DISTRITO  ----------------------------- #
@app.callback(
    Output("id_distrito", "options"),
    Input("id_periodo", "value"),
    Input("id_departamento", "value"),
    Input("id_provincia", "value"))
def distrito_options(periodo_value,departamento_value,provincia_value):
    df_dist = df[df['PERIODO'] == periodo_value]
    df_dist = df_dist[df_dist['DEPARTAMENTO'] == departamento_value]
    df_dist = df_dist[df_dist['PROVINCIA'] == provincia_value]
    return [{'label':i, 'value':i} for i in df_dist['DISTRITO'].unique()]
# Valor inicial del distrito
@app.callback(
    Output("id_distrito", "value"), 
    Input("id_distrito", "options"))
def distrito_values(dis_opt):
    return [[k['value'] for k in dis_opt][0]]

#-----------------------------  TECNOLOGIA  -----------------------------#
# COBERTURA
@app.callback(
    Output("id_tipo_elemento", "options"),
    Input("id_periodo", "value"),
    Input("id_departamento", "value"),
    Input("id_provincia", "value"),
    Input("id_distrito", "value"))
def cobertura_options(periodo_value,departamento_value,provincia_value,distrito_value):
    df_cob = df[df['PERIODO'] == periodo_value]
    df_cob = df_cob[df_cob['DEPARTAMENTO'] == departamento_value]
    df_cob = df_cob[df_cob['PROVINCIA'] == provincia_value]
    df_cob = df_cob[df_cob["DISTRITO"].isin(distrito_value)]
    return [{'label':i, 'value':i} for i in df_cob['TECNOLOGIA'].unique()]
# Valor inicial de la Cobertura
@app.callback(
    Output("id_tipo_elemento", "value"), 
    Input("id_tipo_elemento", "options"))
def cobertura_values(cob_opt):
    return [[k['value'] for k in cob_opt][0]]

#-----------------------------  SEMAFORO  ----------------------------- #
@app.callback(
    Output("id_semaforo", "options"),
    Input("id_periodo", "value"),
    Input("id_departamento", "value"),
    Input("id_provincia", "value"),
    Input('id_distrito','value'),
    Input('id_decil','value'),
    Input('id_tipo_elemento','value'))

def carga_semaforo(periodo_value,departamento_value,provincia_value,distrito_value,range_slider,id_tipo_elemento):
    df_dist = df[df['PERIODO'] == periodo_value]
    df_dist = df_dist[df_dist['DEPARTAMENTO'] == departamento_value]
    df_dist = df_dist[df_dist['PROVINCIA'] == provincia_value]
    df_dist = df_dist[df_dist["DISTRITO"].isin(distrito_value)]
    df_dist = df_dist[df_dist["TECNOLOGIA"].isin(id_tipo_elemento)]
    df_dist = df_dist[ (df_dist['DECIL_BI_2']>=range_slider[0]) & (df_dist['DECIL_BI_2']<=range_slider[1]) ]
    return [{'label':i, 'value':i} for i in df_dist['SEMAFORO'].unique()]

@app.callback(
    Output("id_semaforo", "value"), 
    Input("id_semaforo", "options"))
def semaforo_value(semaforo_opt):
    return [k['value'] for k in semaforo_opt]



#-----------------------------  DESCARGA CSV  ----------------------------- #
# Para la descarga CSV
@app.callback(
    Output(component_id='id_descarga_csv',component_property='data'),
    Input(component_id='id_periodo',component_property='value'),
    Input(component_id='id_departamento',component_property='value'),
    Input(component_id='id_provincia',component_property='value'),
    Input(component_id='id_distrito',component_property='value'),
    Input(component_id='id_decil',component_property='value'),
    
    Input(component_id='id_tipo_elemento',component_property='value'),
    Input(component_id='id_semaforo',component_property='value'),
    
    Input(component_id='id_descarga_boton_csv',component_property='n_clicks'),
    prevent_initial_call=True)
def data_csv(periodo_value,departamento_value,provincia_value,distrito_value,range_slider,tipo_elemento,semaforo,boton_csv):
    df_fin = df[df["PERIODO"] == periodo_value]
    df_fin = df_fin[df_fin['DEPARTAMENTO'] == departamento_value]
    df_fin = df_fin[df_fin['PROVINCIA'] == provincia_value]
    df_fin = df_fin[df_fin["DISTRITO"].isin(distrito_value)]    
    df_fin = df_fin[ (df_fin['DECIL_BI_2']>=range_slider[0]) & (df_fin['DECIL_BI_2']<=range_slider[1]) ]
    
    df_fin = df_fin[df_fin["TECNOLOGIA"].isin(tipo_elemento)]
    df_fin = df_fin[df_fin["SEMAFORO"].isin(semaforo)]
    
    df_fin = df_fin.loc[:,['ELEMENTO','PERIODO','DEPARTAMENTO','PROVINCIA','DISTRITO','DECIL_BI_2','TIPO_ELEMENTO','SEMAFORO']]
    triggered_id = ctx.triggered_id
    if ctx.triggered_id == 'id_descarga_boton_csv':
         return dcc.send_data_frame(df_fin.to_csv, "direc_data.csv", encoding='utf-8',index=False)
        
        
#-----------------------------  DESCARGA EXCEL  ----------------------------- #
@app.callback(
    Output(component_id='id_descarga_excel',component_property='data'),
    Input(component_id='id_periodo',component_property='value'),
    Input(component_id='id_departamento',component_property='value'),
    Input(component_id='id_provincia',component_property='value'),
    Input(component_id='id_distrito',component_property='value'),
    Input(component_id='id_decil',component_property='value'),
    Input(component_id='id_tipo_elemento',component_property='value'),
    Input(component_id='id_semaforo',component_property='value'),
    Input(component_id='id_descarga_boton_excel',component_property='n_clicks'),
    prevent_initial_call=True)
def data_excel(periodo_value,departamento_value,provincia_value,distrito_value,range_slider,tipo_elemento,semaforo,boton_excel):
    df_fin = df[df["PERIODO"] == periodo_value]
    df_fin = df_fin[df_fin['DEPARTAMENTO'] == departamento_value]
    df_fin = df_fin[df_fin['PROVINCIA'] == provincia_value]
    df_fin = df_fin[df_fin["DISTRITO"].isin(distrito_value)]
    df_fin = df_fin[ (df_fin['DECIL_BI_2']>=range_slider[0]) & (df_fin['DECIL_BI_2']<=range_slider[1]) ]
    df_fin = df_fin[df_fin["TECNOLOGIA"].isin(tipo_elemento)]
    df_fin = df_fin[df_fin["SEMAFORO"].isin(semaforo)]
    df_fin = df_fin.loc[:,['ELEMENTO','PERIODO','DEPARTAMENTO','PROVINCIA','DISTRITO','DECIL_BI_2','TIPO_ELEMENTO','SEMAFORO']]
    #triggered_id = ctx.triggered_id
    if ctx.triggered_id == 'id_descarga_boton_excel':
         return dcc.send_data_frame(df_fin.to_excel, "direc_data.xlsx", sheet_name="Data", encoding='utf-8',index=False)

#-----------------------------  MAPA  ----------------------------- #
# Creando el mapa
@app.callback(
    Output(component_id='id_mapa',component_property='figure'),
    Input(component_id='id_periodo',component_property='value'),
    Input(component_id='id_departamento',component_property='value'),
    Input(component_id='id_provincia',component_property='value'),
    Input(component_id='id_distrito',component_property='value'),
    Input(component_id='id_decil',component_property='value'),
    Input(component_id='id_semaforo',component_property='value'),
    Input(component_id='id_tipo_elemento',component_property='value'))

def update_graph(periodo_value,departamento_value,provincia_value,distrito_value,range_slider,id_semaforo,id_tipo_elemento):
    df_mapa = df[df["PERIODO"] == periodo_value]
    df_mapa = df_mapa[df_mapa['DEPARTAMENTO'] == departamento_value]
    df_mapa = df_mapa[df_mapa['PROVINCIA'] == provincia_value]
    df_mapa = df_mapa[df_mapa["DISTRITO"].isin(distrito_value)]
    df_mapa = df_mapa[df_mapa["TECNOLOGIA"].isin(id_tipo_elemento)]
    df_mapa = df_mapa[ (df_mapa['DECIL_BI_2']>=range_slider[0]) & (df_mapa['DECIL_BI_2']<=range_slider[1])]
    df_mapa = df_mapa[df_mapa["SEMAFORO"].isin(id_semaforo)]
    
    df_mapa = gpd.GeoDataFrame(df_mapa)
    # Artificio para centrar el mapa
    lon_mapa = (df_mapa['X_CENTROIDE'].max()+df_mapa['X_CENTROIDE'].min())/2
    lat_mapa = (df_mapa['Y_CENTROIDE'].max()+df_mapa['Y_CENTROIDE'].min())/2
    # Graficando el mapa
    fig = px.choropleth_mapbox(df_mapa,
                               geojson=df_mapa.GEOMETRIA,
                               locations=df_mapa.index,
                               color=df_mapa.SEMAFORO,
                               center={"lat": lat_mapa, "lon": lon_mapa},
                               color_discrete_map={'Ambar':'#F7DC6F','Bloqueado':'#85929E ', 'Rojo':'#E74C3C','Verde':'#27AE60'},
                               mapbox_style="open-street-map",
                               opacity  = 0.4,
                               hover_name="ELEMENTO", 
                               hover_data= {'P_Penetracion':':.2f'},
                               zoom=13)
    fig.update_layout(height=500, margin={"r":2.5,"t":5,"l":0,"b":5})
    fig.update_layout(legend=dict(y=0.99, x=0.005))
    
    return fig

PORT = 8014
#ADDRESS = '127.0.0.1'
ADDRESS = '10.4.88.36'

if __name__ == '__main__':    
    app.run(port=PORT, host=ADDRESS)


# In[ ]:





# In[ ]:


def data_csv(periodo_value,departamento_value,provincia_value,distrito_value,range_slider,boton_csv):
    df_fin = df[df["PERIODO"] == periodo_value]
    df_fin = df_fin[df_fin['DEPARTAMENTO'] == departamento_value]
    df_fin = df_fin[df_fin['PROVINCIA'] == provincia_value]
    df_fin = df_fin[df_fin["DISTRITO"].isin(distrito_value)]
    df_fin = df_fin[ (df_fin['DECIL_BI_2']>=range_slider[0]) & (df_fin['DECIL_BI_2']<=range_slider[1]) ]
    df_fin = df_fin.loc[:,['ELEMENTO','PERIODO','DEPARTAMENTO','PROVINCIA','DISTRITO','DECIL_BI_2','TIPO_ELEMENTO']]
    triggered_id = ctx.triggered_id
    if ctx.triggered_id == 'id_descarga_boton_csv':
         return dcc.send_data_frame(df_fin.to_csv, "direc_data.csv", encoding='utf-8',index=False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# ## Generando el DASH para los MAPAS

# In[ ]:


logger.info('15. PAGINA:MAPA')
# Inicial el Dash
app = Dash(__name__)

# Creando la interfase
app.layout = html.Div([
    # Titulo
    html.H2("MAPA DIRECCIONADOR", style={'text-align':'center', 'color': colors["background"]}),
    # FILA 01
    html.Div(children=[
        # Periodo
        html.Div([
            html.Label('Periodo', style={'color': colors['background']}),
            dcc.Dropdown(id='id_periodo',
                         value = df["PERIODO"].max(),
                         options = [{'label':c,'value':c}
                                    for c in  df["PERIODO"].unique()])
        ], style={'width': '12%','display': 'inline-block'}),
        # Departamento
        html.Div([
            html.Label('Departamento', style={'color': colors['background']}),
            dcc.Dropdown(id='id_departamento',
                         options = [])
        ], style={'width': '19%','display': 'inline-block'}),
        # Provincia
        html.Div([
            html.Label('Provincia', style={'color': colors['background']}),
            dcc.Dropdown(id='id_provincia',
                         options = [])
        ], style={'width': '19%','display': 'inline-block'}),
        # Distrito
        html.Div([
            html.Label('Distrito', style={'color': colors['background']}),
            dcc.Dropdown(id='id_distrito',
                         options = [],
                         multi = True)
        ], style={'width': '30%','display': 'inline-block'}),
        # Descargar CSV
        html.Div([
            html.Label('Descargar CSV', style={'color': colors['background']}),
            html.Button("Click Aqui!", id="id_descarga_boton_csv"),
            dcc.Download(id="id_descarga_csv"),
        ], style={'width': '10%','display': 'inline-block'}),
        # Descargar EXCEL
        html.Div([
            html.Label('Descargar EXCEL', style={'color': colors['background']}),
            html.Button("Click Aqui!", id="id_descarga_boton_excel"),
            dcc.Download(id="id_descarga_excel"),
        ], style={'width': '10%','display': 'inline-block'}),
    ]),

    # FILA 02
    html.Label('Deciles BI', style={'color': colors['background']}),
    dcc.RangeSlider(id='id_decil',
                    min=1,
                    max=10,
                    step = 1,
                    count = 1,
                    value=[1,10]),
    # FILA 03
    html.Div(children=[
        dcc.Graph(id="id_figura_conver", style={'width': '50%','display': 'inline-block'}),
        dcc.Graph(id="id_figura_satura", style={'width': '50%','display': 'inline-block'})
    ]),

    # FILA 04
    html.Div(children=[
        # Cobertura
        html.Div([
            html.Label('Cobertura', style={'color': colors['background']}),
            dcc.Checklist(id='id_tipo_elemento',
                          options = [])
        ], style={'width': '31%','display': 'inline-block'}),
        # Convertibilidad
        html.Div([
            html.Label('Convertibilidad', style={'color': colors['background']}),
            dcc.Checklist(id='id_tipo_conver',
                          options = [])
        ], style={'width': '30%','display': 'inline-block'}),
        # Saturacion
        html.Div([
            html.Label('Saturacion', style={'color': colors['background']}),
            dcc.Checklist(id='id_satu',
                          options = [])
        ], style={'width': '30%','display': 'inline-block'}),
    ])
    
])

# DEPARTAMENTO
@app.callback(
    Output("id_departamento", "options"),
    Input("id_periodo", "value"))
def departamento_options(periodo_value):
    df_dep = df[df['PERIODO'] == periodo_value]
    return [{'label':i, 'value':i} for i in df_dep['DEPARTAMENTO'].unique()]
# Valor inicial del departamento
@app.callback(
    Output("id_departamento", "value"), 
    Input("id_departamento", "options"))
def departamento_values(dep_opt):
    return [k['value'] for k in dep_opt][0]

# PROVINCIA
@app.callback(
    Output("id_provincia", "options"), 
    Input("id_periodo", "value"),
    Input("id_departamento", "value"))
def provincia_options(periodo_value,departamento_value):
    df_prov = df[df['PERIODO'] == periodo_value]
    df_prov = df_prov[df_prov['DEPARTAMENTO'] == departamento_value]
    return [{'label':i, 'value':i} for i in df_prov['PROVINCIA'].unique()]
# Valor inicial de la provincia
@app.callback(
    Output("id_provincia", "value"), 
    Input("id_provincia", "options"))
def provincia_values(prov_opt):
    return [k['value'] for k in prov_opt][0]

# DISTRITO
@app.callback(
    Output("id_distrito", "options"),
    Input("id_periodo", "value"),
    Input("id_departamento", "value"),
    Input("id_provincia", "value"))
def distrito_options(periodo_value,departamento_value,provincia_value):
    df_dist = df[df['PERIODO'] == periodo_value]
    df_dist = df_dist[df_dist['DEPARTAMENTO'] == departamento_value]
    df_dist = df_dist[df_dist['PROVINCIA'] == provincia_value]
    return [{'label':i, 'value':i} for i in df_dist['DISTRITO'].unique()]
# Valor inicial del distrito
@app.callback(
    Output("id_distrito", "value"), 
    Input("id_distrito", "options"))
def distrito_values(dis_opt):
    return [[k['value'] for k in dis_opt][0]]

# COBERTURA
@app.callback(
    Output("id_tipo_elemento", "options"),
    Input("id_periodo", "value"),
    Input("id_departamento", "value"),
    Input("id_provincia", "value"),
    Input("id_distrito", "value"),
    Input('id_decil', 'value'))
def cobertura_options(periodo_value,departamento_value,provincia_value,distrito_value,range_slider):
    df_cob = df[df['PERIODO'] == periodo_value]
    df_cob = df_cob[df_cob['DEPARTAMENTO'] == departamento_value]
    df_cob = df_cob[df_cob['PROVINCIA'] == provincia_value]
    df_cob = df_cob[df_cob["DISTRITO"].isin(distrito_value)]
    df_cob = df_cob[ (df_cob['DECIL_BI_2']>=range_slider[0]) & (df_cob['DECIL_BI_2']<=range_slider[1]) ]
    return [{'label':i, 'value':i} for i in df_cob['TIPO_ELEMENTO'].unique()]
# Valor inicial de la Cobertura
@app.callback(
    Output("id_tipo_elemento", "value"), 
    Input("id_tipo_elemento", "options"))
def cobertura_values(cob_opt):
    return [[k['value'] for k in cob_opt][0]]

# CONVERTIBILIDAD
@app.callback(
    Output("id_tipo_conver", "options"),
    Input("id_periodo", "value"),
    Input("id_departamento", "value"),
    Input("id_provincia", "value"),
    Input("id_distrito", "value"),
    Input("id_tipo_elemento", "value"),
    Input('id_decil', 'value'))
def conver_options(periodo_value,departamento_value,provincia_value,distrito_value,cober_value,range_slider):
    df_conv = df[df['PERIODO'] == periodo_value]
    df_conv = df_conv[df_conv['DEPARTAMENTO'] == departamento_value]
    df_conv = df_conv[df_conv['PROVINCIA'] == provincia_value]
    df_conv = df_conv[df_conv["DISTRITO"].isin(distrito_value)]
    df_conv = df_conv[df_conv["TIPO_ELEMENTO"].isin(cober_value)]
    df_conv = df_conv[ (df_conv['DECIL_BI_2']>=range_slider[0]) & (df_conv['DECIL_BI_2']<=range_slider[1]) ]
    return [{'label':i, 'value':i} for i in df_conv['TIPO_CONVER'].unique()]
# Valor inicial de la Convertibilidad
@app.callback(
    Output("id_tipo_conver", "value"), 
    Input("id_tipo_conver", "options"))
def convertibilidad_values(conv_opt):
    return [k['value'] for k in conv_opt]

# SATURACION
@app.callback(
    Output("id_satu", "options"),
    Input("id_periodo", "value"),
    Input("id_departamento", "value"),
    Input("id_provincia", "value"),
    Input("id_distrito", "value"),
    Input("id_tipo_elemento", "value"),
    Input("id_tipo_conver", "value"),
    Input('id_decil', 'value'))
def conver_options(periodo_value,departamento_value,provincia_value,distrito_value,cober_value,conver_value,range_slider):
    df_satu = df[df['PERIODO'] == periodo_value]
    df_satu = df_satu[df_satu['DEPARTAMENTO'] == departamento_value]
    df_satu = df_satu[df_satu['PROVINCIA'] == provincia_value]
    df_satu = df_satu[df_satu["DISTRITO"].isin(distrito_value)]
    df_satu = df_satu[df_satu["TIPO_ELEMENTO"].isin(cober_value)]
    df_satu = df_satu[df_satu["TIPO_CONVER"].isin(conver_value)]
    df_satu = df_satu[ (df_satu['DECIL_BI_2']>=range_slider[0]) & (df_satu['DECIL_BI_2']<=range_slider[1]) ]
    return [{'label':i, 'value':i} for i in df_satu['TIPO_SATU'].unique()]
# Valor inicial de la Convertibilidad
@app.callback(
    Output("id_satu", "value"), 
    Input("id_satu", "options"))
def saturacion_values(satu_opt):
    return [k['value'] for k in satu_opt]

# Creando el mapa
@app.callback(
    Output(component_id='id_figura_conver',component_property='figure'),
    Output(component_id='id_figura_satura',component_property='figure'),
    Input(component_id='id_periodo',component_property='value'),
    Input(component_id='id_departamento',component_property='value'),
    Input(component_id='id_provincia',component_property='value'),
    Input(component_id='id_distrito',component_property='value'),
    Input(component_id='id_decil',component_property='value'),
    Input(component_id='id_tipo_elemento',component_property='value'),
    Input(component_id='id_tipo_conver',component_property='value'),
    Input(component_id='id_satu',component_property='value'))
def update_graph(periodo_value,departamento_value,provincia_value,distrito_value,range_slider,cobertura_value,conver_value,
                 satu_value):
    df_mapa = df[df["PERIODO"] == periodo_value]
    df_mapa = df_mapa[df_mapa['DEPARTAMENTO'] == departamento_value]
    df_mapa = df_mapa[df_mapa['PROVINCIA'] == provincia_value]
    df_mapa = df_mapa[df_mapa["DISTRITO"].isin(distrito_value)]
    df_mapa = df_mapa[ (df_mapa['DECIL_BI_2']>=range_slider[0]) & (df_mapa['DECIL_BI_2']<=range_slider[1]) ]
    df_mapa = df_mapa[df_mapa["TIPO_ELEMENTO"].isin(cobertura_value)]
    df_mapa = df_mapa[df_mapa["TIPO_CONVER"].isin(conver_value)]
    df_mapa = df_mapa[df_mapa["TIPO_SATU"].isin(satu_value)]
    df_mapa = gpd.GeoDataFrame(df_mapa)
    # Artificio para centrar el mapa
    lon_mapa = (df_mapa['X_CENTROIDE'].max()+df_mapa['X_CENTROIDE'].min())/2
    lat_mapa = (df_mapa['Y_CENTROIDE'].max()+df_mapa['Y_CENTROIDE'].min())/2
    # Graficando el mapa
    fig = px.choropleth_mapbox(df_mapa,
                               geojson=df_mapa.GEOMETRIA,
                               locations=df_mapa.index,
                               color=df_mapa.TIPO_CONVER,
                               center={"lat": lat_mapa, "lon": lon_mapa},
                               color_discrete_map={'Sin Info':'blue','[0 a 0.5>':'red', '[0.5 a 0.7>':'goldenrod','[0.7 a 1]':'Green'},
                               mapbox_style="open-street-map",
                               opacity  = 0.4,
                               zoom=13)
    fig.update_layout(height=350, margin={"r":2.5,"t":5,"l":0,"b":5})
    fig.update_layout(legend=dict(y=0.99, x=0.005))
    
    fig_2 = px.choropleth_mapbox(df_mapa,
                               geojson=df_mapa.GEOMETRIA,
                               locations=df_mapa.index,
                               color=df_mapa.TIPO_SATU,
                               center={"lat": lat_mapa, "lon": lon_mapa},
                               color_discrete_map={'BUENO':'green','MALO':'goldenrod', 'CRITICO':'red'},
                               #color_continuous_scale="Viridis",
                               #range_color=(0, 1),
                               mapbox_style="open-street-map",
                               opacity  = 0.4,
                               zoom=13)
    fig_2.update_layout(height=350, margin={"r":0,"t":5,"l":2.5,"b":5})
    fig_2.update_layout(legend=dict(y=0.99, x=0.005))
    return fig,fig_2

# Para la descarga CSV
@app.callback(
    Output(component_id='id_descarga_csv',component_property='data'),
    Input(component_id='id_periodo',component_property='value'),
    Input(component_id='id_departamento',component_property='value'),
    Input(component_id='id_provincia',component_property='value'),
    Input(component_id='id_distrito',component_property='value'),
    Input(component_id='id_decil',component_property='value'),
    Input(component_id='id_tipo_elemento',component_property='value'),
    Input(component_id='id_tipo_conver',component_property='value'),
    Input(component_id='id_satu',component_property='value'),
    Input(component_id='id_descarga_boton_csv',component_property='n_clicks'),
    prevent_initial_call=True)
def data_csv(periodo_value,departamento_value,provincia_value,distrito_value,range_slider,cobertura_value,conver_value,
                satu_value,boton_csv):
    df_fin = df[df["PERIODO"] == periodo_value]
    df_fin = df_fin[df_fin['DEPARTAMENTO'] == departamento_value]
    df_fin = df_fin[df_fin['PROVINCIA'] == provincia_value]
    df_fin = df_fin[df_fin["DISTRITO"].isin(distrito_value)]
    df_fin = df_fin[ (df_fin['DECIL_BI_2']>=range_slider[0]) & (df_fin['DECIL_BI_2']<=range_slider[1]) ]
    df_fin = df_fin[df_fin["TIPO_ELEMENTO"].isin(cobertura_value)]
    df_fin = df_fin[df_fin["TIPO_CONVER"].isin(conver_value)]
    df_fin = df_fin[df_fin["TIPO_SATU"].isin(satu_value)]
    df_fin = df_fin.loc[:,['ELEMENTO','PERIODO','DEPARTAMENTO','PROVINCIA','DISTRITO','DECIL_BI_2','TIPO_ELEMENTO',
                                'P_CONVER_U30D','TIPO_CONVER','SATURACION','TIPO_SATU']]
    triggered_id = ctx.triggered_id
    if ctx.triggered_id == 'id_descarga_boton_csv':
         return dcc.send_data_frame(df_fin.to_csv, "direc_data.csv", encoding='utf-8',index=False)

# Para la descarga EXCEL
@app.callback(
    Output(component_id='id_descarga_excel',component_property='data'),
    Input(component_id='id_periodo',component_property='value'),
    Input(component_id='id_departamento',component_property='value'),
    Input(component_id='id_provincia',component_property='value'),
    Input(component_id='id_distrito',component_property='value'),
    Input(component_id='id_decil',component_property='value'),
    Input(component_id='id_tipo_elemento',component_property='value'),
    Input(component_id='id_tipo_conver',component_property='value'),
    Input(component_id='id_satu',component_property='value'),
    Input(component_id='id_descarga_boton_excel',component_property='n_clicks'),
    prevent_initial_call=True)
def data_excel(periodo_value,departamento_value,provincia_value,distrito_value,range_slider,cobertura_value,conver_value,
                satu_value,boton_excel):
    df_fin = df[df["PERIODO"] == periodo_value]
    df_fin = df_fin[df_fin['DEPARTAMENTO'] == departamento_value]
    df_fin = df_fin[df_fin['PROVINCIA'] == provincia_value]
    df_fin = df_fin[df_fin["DISTRITO"].isin(distrito_value)]
    df_fin = df_fin[ (df_fin['DECIL_BI_2']>=range_slider[0]) & (df_fin['DECIL_BI_2']<=range_slider[1]) ]
    df_fin = df_fin[df_fin["TIPO_ELEMENTO"].isin(cobertura_value)]
    df_fin = df_fin[df_fin["TIPO_CONVER"].isin(conver_value)]
    df_fin = df_fin[df_fin["TIPO_SATU"].isin(satu_value)]
    df_fin = df_fin.loc[:,['ELEMENTO','PERIODO','DEPARTAMENTO','PROVINCIA','DISTRITO','DECIL_BI_2','TIPO_ELEMENTO',
                                'P_CONVER_U30D','TIPO_CONVER','SATURACION','TIPO_SATU']]
    #triggered_id = ctx.triggered_id
    if ctx.triggered_id == 'id_descarga_boton_excel':
         return dcc.send_data_frame(df_fin.to_excel, "direc_data.xlsx", sheet_name="Data", encoding='utf-8',index=False)

        
# Final del Dash
# change these values
PORT = 8000
#ADDRESS = '127.0.0.1'
ADDRESS = '10.4.88.36'

if __name__ == '__main__':    
    app.run(port=PORT, host=ADDRESS)


# In[ ]:


print("Fin del proceso de modelo de direccionador + mapas")
print(" ")


# In[ ]:





# In[ ]:




