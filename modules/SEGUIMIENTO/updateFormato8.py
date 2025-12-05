import sys
sys.path.insert(0, './conn')
import psycopg2
import requests
from bs4 import BeautifulSoup
import time
from conn import Conexion

# ========================================
# 🔹 FUNCIÓN: Obtener etapa/estado del MEF
# ========================================
def obtener_datos(codigo):
    url = f"https://ofi5.mef.gob.pe/invierte/ejecucion/verFichaEjecucion/{codigo}"
    headers = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding':'gzip, deflate, sdch',
            'Accept-Language':'es-419,es;q=0.8',
            'Cache-Control':'no-cache',
            'Connection':'keep-alive',
            'Cookie':'ASP.NET_SessionId=p023dx0oo4gi1r1j5brbfmdh; _transparencia=_kIdx=0&_vIdx=1|8|9|10|11|12|13|15; nlbi_946632=VGBYHWlVHhyd59Lz7nEzAQAAAACTKAGzvC5Cd6cWV4/34UHG; visid_incap_946632=nMEhlA37RV+OLuCyarmKdkv5gFkAAAAARUIPAAAAAACA1vN+AcgwgRI+Jar08MLi9wglKX0wwyey; incap_ses_684_946632=CxstcMGMTHU8pYiVaQ5+CRkrxFkAAAAAIMqURI1RFhisPCcd6GOHlw==',
            'Host':'apps5.mineco.gob.pe',
            'Pragma':'no-cache',
            'Referer':'http://apps5.mineco.gob.pe/transparencia/Navegador/default.aspx',
            'Upgrade-Insecure-Requests':'1',
            'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:50.0) Gecko/20100101 Firefox/50.0'
           }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = 'utf-8'
        if response.status_code != 200:
            return None, None

        soup = BeautifulSoup(response.text, 'html.parser')
        div = soup.find('div', style=lambda x: x and '#D9EDF7' in x)
        if not div:
            return None, None

        strongs = div.find_all('strong')
        if len(strongs) >= 2:
            etapa = strongs[0].get_text(strip=True)
            estado = strongs[1].get_text(strip=True)
        else:
            etapa = estado = None

        return etapa, estado

    except Exception as e:
        print(f"⚠️ Error en {codigo}: {e}")
        return None, None


# ========================================
# 🔹 FUNCIÓN: Guardar resultado en BD
# ========================================
def guardar_resultado(conn, codigo, etapa, estado):
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO seguimiento_formatos8 (codigo_unico, etapa, estado, fecha)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (codigo_unico)
            DO UPDATE SET
                etapa = EXCLUDED.etapa,
                estado = EXCLUDED.estado,
                fecha = NOW();
        """, (codigo, etapa, estado))
    except psycopg2.errors.SyntaxError:
        # 🔁 Fallback si no hay soporte para ON CONFLICT
        conn.rollback()
        cur.execute("""
            UPDATE seguimiento_formatos8
            SET etapa = %s,
                estado = %s,
                fecha = NOW()
            WHERE codigo_unico = %s
        """, (etapa, estado, codigo))
        if cur.rowcount == 0:
            cur.execute("""
                INSERT INTO seguimiento_formatos8 (codigo_unico, etapa, estado, fecha)
                VALUES (%s, %s, %s, NOW())
            """, (codigo, etapa, estado))
    conn.commit()
    cur.close()


# ========================================
# 🔹 MAIN
# ========================================
def main():
    con = Conexion()
    con.connect()
    cur = con.getCur()  # Cursor tipo RealDictCursor

    cur.execute(""" SELECT cod_unif
    FROM grli_pip_total_priori
    WHERE cod_unif IS NOT NULL
      AND (
          cod_unif NOT IN (SELECT codigo_unico FROM seguimiento_formatos8)
          OR cod_unif IN (
              SELECT codigo_unico FROM seguimiento_formatos8
              WHERE fecha::date < CURRENT_DATE
          )
      ); """)
    codigos = [row['cod_unif'] for row in cur.fetchall()]  # ✅ corregido aquí
    total = len(codigos)
    print(f"🔍 {total} códigos encontrados para procesar")

    for i, codigo in enumerate(codigos, start=1):
        etapa, estado = obtener_datos(codigo)
        if etapa and estado:
            guardar_resultado(con.getConn(), codigo, etapa, estado)
            print(f"✅ [{i}/{total}] {codigo}: {etapa} - {estado}")
        else:
            print(f"⚠️ [{i}/{total}] {codigo}: No se pudo obtener información")

        # Pequeña pausa cada 10 para no saturar el MEF
        if i % 10 == 0:
            time.sleep(2)

    cur.close()
    con.close()
    print("🎯 Proceso finalizado correctamente")


if __name__ == "__main__":
    main()
