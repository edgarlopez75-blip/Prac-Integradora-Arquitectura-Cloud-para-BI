

from flask import Flask, jsonify, request
import requests
import math
 
app = Flask(__name__)
 

# CONFIGURACIÓN DE SUPABASE REST API

SUPABASE_URL = "https://lqkpvhbuooupzvuvbgsb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imxxa3B2aGJ1b291cHp2dXZiZ3NiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc3NDk0NjYsImV4cCI6MjA5MzMyNTQ2Nn0.rl9hAsLz-sxKJdWH4-r4NayWKyDBqN9RvtjRFYz66OU"
 
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}
 
def supabase_get(tabla, params=None):
    """
    Función auxiliar para hacer GET a la API REST de Supabase.
    Usa HTTPS (puerto 443) que sí está permitido en PythonAnywhere.
    """
    url = f"{SUPABASE_URL}/rest/v1/{tabla}"
    response = requests.get(url, headers=HEADERS, params=params)
    return response.json()
 
 

# ENDPOINT 1: CATÁLOGO GENERAL
# GET /api/unidades

@app.route("/api/unidades", methods=["GET"])
def get_unidades():
    """Retorna listado general con LIMIT 100."""
    params = {
        "select": "id,nombre_establecimiento,razon_social",
        "order":  "id",
        "limit":  100
    }
    datos = supabase_get("establecimientos", params)
    return jsonify({"total": len(datos), "datos": datos})
 
 

# ENDPOINT 2: CONSULTA POR ID
# GET /api/unidades/<id>

@app.route("/api/unidades/<int:id>", methods=["GET"])
def get_unidad_por_id(id):
    """Retorna datos básicos de una unidad por su ID."""
    params = {
        "select": "id,nombre_establecimiento,razon_social,direccion,codigo_postal",
        "id":     f"eq.{id}"
    }
    datos = supabase_get("establecimientos", params)
    if not datos:
        return jsonify({"error": f"No se encontró unidad con ID {id}"}), 404
    return jsonify(datos[0])
 

# ENDPOINT 3: BUSCADOR POR NOMBRE
# GET /api/unidades/buscar?nombre={texto}
@app.route("/api/unidades/buscar", methods=["GET"])
def buscar_por_nombre():
    """Busca establecimientos por nombre usando ILIKE."""
    nombre = request.args.get("nombre", "")
    params = {
        "select":                 "id,nombre_establecimiento,razon_social,direccion",
        "nombre_establecimiento": f"ilike.*{nombre}*",
        "limit":                  50
    }
    datos = supabase_get("establecimientos", params)
    return jsonify({"total": len(datos), "datos": datos})
 
 

# ENDPOINT 4: KPI ESTATAL
# GET /api/estadisticas/total_por_estado

@app.route("/api/estadisticas/total_por_estado", methods=["GET"])
def total_por_estado():
    """
    Retorna total de unidades por estado.
    Agrupa en Python porque Supabase REST no hace GROUP BY directo.
    """
    establecimientos = supabase_get("establecimientos", {
        "select": "id_entidad",
        "limit":  1000
    })
    entidades = supabase_get("cat_entidades", {
        "select": "clave_entidad,nombre_entidad"
    })
 
    mapa = {e["clave_entidad"]: e["nombre_entidad"] for e in entidades}
    conteo = {}
    for est in establecimientos:
        nombre = mapa.get(est["id_entidad"], "Desconocido")
        conteo[nombre] = conteo.get(nombre, 0) + 1
 
    resultado = [{"nombre_entidad": k, "total_unidades": v}
                 for k, v in sorted(conteo.items(), key=lambda x: -x[1])]
    return jsonify({"datos": resultado})
 
 

# ENDPOINT 5: FILTROS MÚLTIPLES DINÁMICOS
# GET /api/unidades/filtro?estado={X}&actividad={Y}

@app.route("/api/unidades/filtro", methods=["GET"])
def filtrar_unidades():
    """
    Filtra por estado y/o actividad.
    Si solo llega estado filtra por estado,
    si llegan ambos filtra por ambos.
    """
    estado    = request.args.get("estado", "").lower()
    actividad = request.args.get("actividad", "").lower()
 
    establecimientos = supabase_get("establecimientos", {
        "select": "id,nombre_establecimiento,razon_social,id_entidad,clave_actividad",
        "limit":  1000
    })
    entidades   = supabase_get("cat_entidades",  {"select": "clave_entidad,nombre_entidad"})
    actividades = supabase_get("cat_actividades", {"select": "codigo_actividad,nombre_actividad"})
 
    mapa_ent = {e["clave_entidad"]:    e["nombre_entidad"]   for e in entidades}
    mapa_act = {a["codigo_actividad"]: a["nombre_actividad"] for a in actividades}
 
    resultados = []
    for est in establecimientos:
        nombre_ent = mapa_ent.get(est["id_entidad"], "").lower()
        nombre_act = mapa_act.get(est["clave_actividad"], "").lower()
        if estado and estado not in nombre_ent:
            continue
        if actividad and actividad not in nombre_act:
            continue
        resultados.append({
            "id":                    est["id"],
            "nombre_establecimiento": est["nombre_establecimiento"],
            "razon_social":          est["razon_social"],
            "nombre_entidad":        mapa_ent.get(est["id_entidad"], ""),
            "nombre_actividad":      mapa_act.get(est["clave_actividad"], "")
        })
 
    return jsonify({"total": len(resultados), "datos": resultados[:100]})
 
 

# ENDPOINT 6: PERFIL COMPLETO ANIDADO
# GET /api/unidades/<id>/perfil_completo

@app.route("/api/unidades/<int:id>/perfil_completo", methods=["GET"])
def perfil_completo(id):
    """
    Perfil completo con JOINs manuales via API REST.
    Retorna JSON jerárquico con sub-objetos.
    """
    datos = supabase_get("establecimientos", {"select": "*", "id": f"eq.{id}"})
    if not datos:
        return jsonify({"error": f"No se encontró unidad con ID {id}"}), 404
 
    est = datos[0]
    entidad   = supabase_get("cat_entidades",  {"select": "nombre_entidad",  "clave_entidad":    f"eq.{est['id_entidad']}"})
    municipio = supabase_get("cat_municipios", {"select": "nombre_municipio", "clave_municipio": f"eq.{est['id_municipio']}", "clave_entidad": f"eq.{est['id_entidad']}"})
    localidad = supabase_get("cat_localidades",{"select": "nombre_localidad", "clave_localidad": f"eq.{est['id_localidad']}", "clave_municipio": f"eq.{est['id_municipio']}", "clave_entidad": f"eq.{est['id_entidad']}"})
    actividad = supabase_get("cat_actividades",{"select": "nombre_actividad,sector", "codigo_actividad": f"eq.{est['clave_actividad']}"})
 
    return jsonify({
        "id": est["id"],
        "nombre_establecimiento": est["nombre_establecimiento"],
        "razon_social": est["razon_social"],
        "ubicacion": {
            "direccion":    est["direccion"],
            "codigo_postal": est["codigo_postal"],
            "localidad":    localidad[0]["nombre_localidad"] if localidad else None,
            "municipio":    municipio[0]["nombre_municipio"] if municipio else None,
            "estado":       entidad[0]["nombre_entidad"]     if entidad   else None,
            "coordenadas":  {"latitud": est["latitud"], "longitud": est["longitud"]}
        },
        "actividad": {
            "nombre": actividad[0]["nombre_actividad"] if actividad else None,
            "sector": actividad[0]["sector"]           if actividad else None
        }
    })
 
 
# ENDPOINT 7: BÚSQUEDA GEOESPACIAL
# GET /api/unidades/cercanas?lat={X}&lon={Y}&radio={Z}

@app.route("/api/unidades/cercanas", methods=["GET"])
def unidades_cercanas():
    """
    Unidades dentro de un radio en km usando Haversine.
    """
    lat   = request.args.get("lat",   type=float)
    lon   = request.args.get("lon",   type=float)
    radio = request.args.get("radio", 10.0, type=float)
 
    if lat is None or lon is None:
        return jsonify({"error": "Se requieren lat y lon"}), 400
 
    todos = supabase_get("establecimientos", {
        "select": "id,nombre_establecimiento,razon_social,latitud,longitud",
        "latitud": "not.is.null",
        "limit":   1000
    })
 
    def haversine(lat1, lon1, lat2, lon2):
        """Distancia en km entre dos coordenadas geográficas."""
        R = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * \
            math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        return R * 2 * math.asin(math.sqrt(a))
 
    cercanos = []
    for est in todos:
        if est["latitud"] and est["longitud"]:
            dist = haversine(lat, lon, est["latitud"], est["longitud"])
            if dist <= radio:
                cercanos.append({
                    "id":                    est["id"],
                    "nombre_establecimiento": est["nombre_establecimiento"],
                    "razon_social":          est["razon_social"],
                    "latitud":               est["latitud"],
                    "longitud":              est["longitud"],
                    "distancia_km":          round(dist, 2)
                })
 
    cercanos.sort(key=lambda x: x["distancia_km"])
    return jsonify({"total": len(cercanos), "radio_km": radio, "datos": cercanos})
 
 
if __name__ == "__main__":
    app.run(debug=True)
 