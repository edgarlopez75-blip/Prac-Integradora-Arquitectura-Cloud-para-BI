import pandas as pd 
import numpy as np 
from sqlalchemy import create_engine, text

#configuración de conexión 
DB_HOST="aws-1-us-west-1.pooler.supabase.com"
DB_PORT="5432"
DB_NAME="postgres"
DB_USER="postgres.lqkpvhbuooupzvuvbgsb"
DB_PASSWORD="Eelocbta198"

EXCEL_PATH="DENUE_Normalizado.xlsx"


#conexión a supabase 
print("conectando a Supabase")
connection_string=f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine=create_engine(connection_string)

#paso 1 leer el Excel 
#porque primero hearde=1 es la fila 0 es el título decorativo del Excel 

print("leyendo el Excel")

df_estab=pd.read_excel(EXCEL_PATH,sheet_name="Establecimientos",header=1).dropna(how="all")
df_act=pd.read_excel(EXCEL_PATH,sheet_name="CAT_Actividades",header=1).dropna(how="all")
df_ent=pd.read_excel(EXCEL_PATH,sheet_name="CAT_Entidades",header=1).dropna(how="all")
df_mun=pd.read_excel(EXCEL_PATH,sheet_name="CAT_Municipios",header=1).dropna(how="all")
df_loc=pd.read_excel(EXCEL_PATH,sheet_name="CAT_Localidades",header=1).dropna(how="all")
df_personal=pd.read_excel(EXCEL_PATH,sheet_name="CAT_PersonalOcupado",header=1).dropna(how="all")

print(f"Establecimientos:{len(df_estab)} registros")
print(f"Actividades:{len(df_act)} registros")
print(f"Entidades:{len(df_ent)} registros")
print(f"Municipios:{len(df_mun)} registros")
print(f"Localidades:{len(df_loc)} registros")
print(f"Personal:{len(df_personal)} registros")

#paso 2 limpiar y renombrar columnas 
#más sencillo convertí nombres con espacios y acentos a snake case limpio 

print("Limpiando columnas")

# Catálogo de Entidades 
df_ent.columns=["clave_entidad","nombre_entidad"]
df_ent["clave_entidad"]=df_ent["clave_entidad"].astype(int)

#Catálogo de Actividades
df_act.columns=["codigo_actividad","nombre_actividad","sector"]
df_act["codigo_actividad"]=df_act["codigo_actividad"].astype(int)
df_act["sector"]=df_act["sector"].astype(int)

#Catálogo de Municipios
df_mun.columns=["clave_municipio","nombre_municipio","clave_entidad"]
df_mun["clave_municipio"]=df_mun["clave_municipio"].astype(int)
df_mun["clave_entidad"]=df_mun["clave_entidad"].astype(int)

#Catálogo de Localidades
df_loc.columns = df_loc.columns.str.strip()
df_loc.columns=["clave_localidad", "nombre_localidad", "clave_municipio", "clave_entidad"]
df_loc["clave_localidad"]=df_loc["clave_localidad"].astype(int)
df_loc["clave_municipio"]=df_loc["clave_municipio"].astype(int)
df_loc["clave_entidad"]=df_loc["clave_entidad"].astype(int)

#Catálogo de Personal Ocupado
df_personal.columns=["rango","estimado_personas","clasificacion_tamaño"]

#Establecimientos es la tabla principal
df_estab.columns=[
    "id","nombre_establecimiento","razon_social",
    "clave_actividad","direccion","codigo_postal",
    "id_localidad","id_municipio","id_entidad"
]

df_estab["id"]              =df_estab["id"].astype(int)
df_estab["clave_actividad"] =df_estab["clave_actividad"].astype(int)
df_estab["id_municipio"]    =df_estab["id_municipio"].astype(int)
df_estab["id_entidad"]      =df_estab["id_entidad"].astype(int)
df_estab["id_localidad"]    =df_estab["id_localidad"].astype(int)
df_estab["codigo_postal"]   =pd.to_numeric(df_estab["codigo_postal"],errors="coerce").astype("Int64")

#paso 3 agregar coordenadas lat y lon dentro de rango de Aguascalientes y Baja California 
print("Agregando coordenadas")

np.random.seed(42)

def generar_coords(entidad_id):
    if entidad_id == 1:  #Aguascalientes 
        lat = np.random.uniform(21.60,22.45)
        lon = np.random.uniform(-102.90,-102.00)
    else:                #Baja California
        lat = np.random.uniform(29.00,32.70)
        lon = np.random.uniform(-117.10,-114.50)
    return lat, lon

coordenadas = df_estab["id_entidad"].apply(generar_coords)
df_estab["latitud"] =[c[0] for c in coordenadas]
df_estab["longitud"]=[c[1] for c in coordenadas] 

#paso 4 creación de tablas en Supabase
print("La creación de tablas en Supabase")

sql_crear_tablas="""
DROP TABLE IF EXISTS establecimientos CASCADE;
DROP TABLE IF EXISTS cat_localidades CASCADE;
DROP TABLE IF EXISTS cat_municipios CASCADE;
DROP TABLE IF EXISTS cat_actividades CASCADE;
DROP TABLE IF EXISTS cat_entidades CASCADE;
DROP TABLE IF EXISTS cat_personal_ocupado CASCADE;

CREATE TABLE cat_entidades (
    clave_entidad INTEGER PRIMARY KEY,
    nombre_entidad VARCHAR(100) NOT NULL
);

CREATE TABLE cat_actividades (
    codigo_actividad INTEGER PRIMARY KEY,
    nombre_actividad VARCHAR(255) NOT NULL,
    sector           INTEGER NOT NULL
);

CREATE TABLE cat_municipios (
    clave_municipio INTEGER NOT NULL,
    nombre_municipio VARCHAR(150) NOT NULL,
    clave_entidad INTEGER NOT NULL REFERENCES cat_entidades(clave_entidad),
    PRIMARY KEY (clave_municipio, clave_entidad)
);

CREATE TABLE cat_localidades (
    clave_localidad INTEGER NOT NULL,
    nombre_localidad VARCHAR(200) NOT NULL,
    clave_municipio INTEGER NOT NULL,
    clave_entidad INTEGER NOT NULL REFERENCES cat_entidades(clave_entidad),
    PRIMARY KEY (clave_localidad,clave_municipio,clave_entidad)
);

CREATE TABLE cat_personal_ocupado (
    id                    SERIAL PRIMARY KEY,
    rango                 VARCHAR(50) NOT NULL,
    estimado_personas     INTEGER,
    clasificacion_tamaño  VARCHAR(50)
);

CREATE TABLE establecimientos (
    id                       BIGINT PRIMARY KEY,
    nombre_establecimiento   VARCHAR(255),
    razon_social             VARCHAR(255),
    clave_actividad          INTEGER REFERENCES cat_actividades(codigo_actividad),
    direccion                TEXT,
    codigo_postal            INTEGER,
    id_localidad             INTEGER,
    id_municipio             INTEGER NOT NULL,
    id_entidad               INTEGER NOT NULL REFERENCES cat_entidades(clave_entidad),
    latitud                  DOUBLE PRECISION,
    longitud                 DOUBLE PRECISION,
);
"""

with engine.connect() as conn:
    conn.execute(text(sql_crear_tablas))
    conn.commit()
    print("Tablas creadas correctamente")


#paso 5 cargar datos en orden 


print("Cargando datos")

df_ent.to_sql("cat_entidades",             engine, if_exists="append", index=False)
print("cat_entidades cargada")

df_act.to_sql("cat_actividades",           engine, if_exists="append", index=False)
print("cat_actividades cargada")

df_mun.to_sql("cat_municipios",            engine, if_exists="append", index=False)
print("cat_municipios cargada")
print(df_loc.columns.tolist())
df_loc.to_sql("cat_localidades",           engine, if_exists="append", index=False)
print("cat_localidades cargada")

df_personal.to_sql("cat_personal_ocupado", engine, if_exists="append", index=False)
print("cat_personal_ocupado cargada")

df_estab.to_sql("establecimientos",        engine, if_exists="replace", index=False)
print("establecimientos cargada")

#verificación 
print("Migración completada. Verificando")
with engine.connect() as conn:
    tablas = ["cat_entidades","cat_actividades","cat_municipios",
              "cat_localidades","cat_personal_ocupado","establecimientos"]
    for tabla in tablas:
        resultado = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar()
        print(f"{tabla}:{resultado}registros")
print("Listo, revisa los datos")
