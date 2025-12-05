from datetime import datetime
import reader
# from colorama import Fore

def main(con,years):
   
    def exists(fecha):
        con.getCur().execute("""
        SELECT * FROM inf_financiera_dia
        WHERE inf_financiera_dia.fecha = %s and inf_financiera_dia.anio = %s""",(str(fecha),str(years))
        )
        Finance = con.getCur().fetchall()
        return len(Finance)

    def insert(data):
        for row in data:
            #Nombre Proyecto
            nombreProyecto = row.contents[0].text
            #Codigo Unificado del proyecto
            codigo = nombreProyecto.split(':')[0].strip()
            #Nombre del proyecto
            nombre = nombreProyecto.split(':')[1]

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
            girado = row.contents[7].text.replace(",", "")
            girado = girado.replace(".", ",")
            girado = girado if len(girado) != 0 else 0
            #AVANCE FINANCIERO
            a_financ_dia = row.contents[8].text.replace(",", "")
            a_financ_dia = a_financ_dia if len(a_financ_dia) != 0 else 0
            #a_financ_dia = a_financ_dia.replace(".", ",")
            #Fecha
            fecha = str(datetime.now().date())

            con.getCur().execute("""
                INSERT INTO inf_financiera_dia(cod_unif,pia_dia,pim_dia,certificacion_dia,comp_anual_dia,ate_comp_anual_dia,dev_dia,girado_dia,a_financ_dia,fecha,anio)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                    str(codigo).strip(),
                    str(pia_dia).strip(),
                    str(pim_dia).strip(),
                    str(certificacion_dia).strip(),
                    str(comp_anual_dia).strip(),
                    str(ate_comp_anual_dia).strip(),
                    str(dev_dia).strip(),
                    str(girado).strip(),
                    str(a_financ_dia).strip(),
                    str(fecha).strip(),
                    str(years).strip()
                )
            )

    def __init__():

        fecha = str(datetime.now().date())

        print('Iniciando...')
        if exists(fecha) == 0:
            document = './downloads/'+ str(years) +'/EjecucionPresupuestal/Proyectos/' + str(datetime.now().date()) + '.html'
            data = reader.bs4Html(document)
            trs = data.find('table', class_="Data").contents
            insert(trs)
            print('Guardando -> Tabla [inf_financiera_dia]...')
            con.getConn().commit()
        else:
            print('Ya se Ingreso Informacion de Hoy')

    # Iniciar
    __init__()
