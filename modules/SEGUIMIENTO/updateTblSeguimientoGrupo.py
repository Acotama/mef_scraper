import os
import json
import requester
import reader
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal

# from colorama import Fore

def main(con,years):

    def getultimafecharegistro(denominacion,categoria):
        con.getCur().execute(
        """
            select max(fecha) as fecha from tb_ejecucion_grupo  where anio= %s and denominacion= %s and categoria=%s
        """, (str(years),denominacion,categoria))

        fecha = con.getCur().fetchone()['fecha']
        if fecha == None:
            return datetime.now().date() + timedelta(days=-1)
        return fecha

    def insert (data,fecha,denominacion,categoria):
        for row in data:
            #Nombre Proyecto
            nombreProyecto = row.contents[0].text
            #Codigo Unificado del proyecto
            codigo = nombreProyecto.split(':')[0].strip()
            #Nombre del proyecto
            nombre = nombreProyecto.split(':')[1].strip()
            #PIA
            pia_dia = row.contents[1].text.replace(",", "")
            pia_dia = pia_dia.replace(".", ",")
            pia_dia = pia_dia if len(pia_dia) != 0 else 0 
            #PIM
            pim_dia = row.contents[2].text.replace(",", "")
            pim_dia = pim_dia.replace(".", ",")
            pim_dia = pim_dia if len(pim_dia) != 0 else 0
            #CERTIFICADO
            certificacion_dia = row.contents[3].text.replace(",", "")
            certificacion_dia = certificacion_dia.replace(".", ",")
            certificacion_dia = certificacion_dia if len(certificacion_dia) != 0 else 0
            #COMPROMISO ANUAL
            comp_anual_dia = row.contents[4].text.replace(",", "")
            comp_anual_dia = comp_anual_dia.replace(".", ",")
            comp_anual_dia = comp_anual_dia if len(comp_anual_dia) != 0 else 0
            #ATENCION DEL COMPROMISO ANUAL
            ate_comp_anual_dia = row.contents[5].text.replace(",", "")
            ate_comp_anual_dia = ate_comp_anual_dia.replace(".", ",")
            ate_comp_anual_dia = ate_comp_anual_dia if len(ate_comp_anual_dia) != 0 else 0
            #DEVENGADO POR DIA
            dev_dia = row.contents[6].text.replace(",", "")
            dev_dia = dev_dia.replace(".", ",")
            dev_dia = dev_dia if len(dev_dia) != 0 else 0
            #Girado
            girado_dia = row.contents[7].text.replace(",", "")
            girado_dia = girado_dia.replace(".", ",")
            girado_dia = girado_dia if len(girado_dia) != 0 else 0
            #AVANCE FINANCIERO
            a_financ_dia = row.contents[8].text.replace(",", "")
            a_financ_dia = a_financ_dia if len(a_financ_dia) != 0 else 0
            #a_financ_dia = a_financ_dia.replace(".", ",")
            #Fecha
            fecha = fecha
            con.getCur().execute(
            """
                INSERT INTO tb_ejecucion_grupo(
                codigo_unico,
                nombre,
                pia_dia,  
                pim_dia,  
                certificacion_dia, 
                comp_anual_dia, 
                ate_comp_anual_dia,
                dev_dia,
                girado_dia, 
                a_financ_dia,
                fecha, 
                anio,
                denominacion,
                categoria
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
            """,
                (
                    str(codigo),
                    str(nombre),
                    str(pia_dia),
                    str(pim_dia),
                    str(certificacion_dia),
                    str(comp_anual_dia),
                    str(ate_comp_anual_dia),
                    str(dev_dia),
                    str(girado_dia),
                    str(a_financ_dia),
                    str(fecha),
                    str(years),
                    str(denominacion),
                    str(categoria)
                )
            )

    def __init__():
        print("INICIANDO...")
        unidadesEjecutoras = {"Funcion"}
        for grupo in unidadesEjecutoras:
            carpeta = "./downloads/"+ str(years) + "/EjecucionPresupuestal/" + grupo
            files_c = os.listdir(carpeta)
            file_array = []
            for fichero in files_c:
                if fichero.find("html") < 0 :
                    file_array.append(fichero)
            files_c = file_array
            for files_c_f in files_c:
                files = os.listdir("./downloads/"+ str(years) + "/EjecucionPresupuestal/"+ grupo + "/" + files_c_f)
                if len(files) > 0:
                    fechaultregistro_grupo = datetime.strptime(str(getultimafecharegistro(grupo,files_c_f)), '%Y-%m-%d').date()
                    print ("INGRESANDO " + grupo +" - " +  files_c_f + " DE "+ str(fechaultregistro_grupo) +" A "+str(datetime.now().date()))
                    file = max(files)
                    fecha = datetime.strptime(file.split('.')[0], '%Y-%m-%d').date()
                    if fecha > fechaultregistro_grupo:
                        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ACTUALIZANDO A LA FECHA => " + str(fecha) + " TABLA [tb_ejecucion_grupo] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
                        data = reader.bs4Html("./downloads/"+ str(years) + "/EjecucionPresupuestal/"+ grupo + "/" + files_c_f + "/" + str(file))
                        trs = data.find("table", class_="Data").contents
                        insert( trs, file.split(".")[0],grupo,files_c_f)
                        print("------> DATOS INGRESADOS")
                    else:
                        print("DATOS YA ACTUALIZADOS " + str(fecha) + " Tabla [tb_ejecucion_grupo]...")
        con.getConn().commit()

    # Iniciar
    __init__()
