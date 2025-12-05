import os
import json
import requester
import reader
from datetime import datetime, timedelta
from decimal import Decimal

# from colorama import Fore

def main(con,years):

    def getformato12b():
        con.getCur().execute("""  select distinct cod_unif from vw_lista_formato1b_extraer where cod_unif not in('2413364','2332433')  """)
        return  con.getCur().fetchall()
    
    def lista_formato_12b():
        con.getCur().execute("""select distinct cod_unif from formato12b where fecha_subida is null """)
        resultados =   con.getCur().fetchall()
        resultados_lista = [row['cod_unif'] for row in resultados]
        return resultados_lista
        # con.getConn().commit()

    def deleteformato12b(fecha):
        con.getCur().execute("""delete  from formato12b fb where fecha_subida = %s; """,(str(fecha),))
        # con.getConn().commit()

    def getultimafecharegistro():
        # con.getCur().execute("""
        # select max(fecha) as fecha from grli_pip_seguimiento_ejecucion_financiera
        # where anio= %s""", (str(years),))
        # fecha = con.getCur().fetchone()['fecha']

        # if fecha == None:
        #     return datetime.now().date()
            
        # return fecha

        con.getCur().execute("""
        select max(fecha) as fecha from grli_pip_seguimiento_ejecucion_financiera
        where anio= %s""", (str(years),))
        fecha = con.getCur().fetchone()['fecha']
            
        if fecha == None:
            return datetime.now().date() + timedelta(days=-1)
            
        return fecha

    def getUltimoRegistro(cod_unif, fecha): 
        con.getCur().execute("""
            SELECT * FROM grli_pip_seguimiento_ejecucion_financiera
            WHERE cod_unif = %s and fuente_financiamiento is NULL and anio =%s
            ORDER BY fecha desc
            LIMIT 1
            """, (cod_unif,str(years)))

        ultimoRegistro = con.getCur().fetchone()

        if ultimoRegistro != None:
            if (datetime.strptime(fecha, '%Y-%m-%d').date() - ultimoRegistro['fecha']).days == 1:
                return ultimoRegistro

        return False

    def getultimafecharegistro_mensualisado(mes):
        con.getCur().execute("""
        select max(fecha) as fecha from inf_financiera2
        where anio= %s and mes = %s""", (str(years),str(mes)))

        fecha = con.getCur().fetchone()['fecha']

        if fecha == None:
            return datetime.now().date() + timedelta(days=-1)
            
        return fecha

    def dateExists(fecha):
        con.getCur().execute("""
            SELECT * FROM grli_pip_seguimiento_ejecucion_financiera
            WHERE fecha = %s and anio = %s
            """, (fecha,str(years)))    

        if len(con.getCur().fetchall()) > 0 :
            return True
        return False

    def dateExists_Formato12b(fecha):
        con.getCur().execute("""
            SELECT * FROM formato12b
            WHERE fecha_subida = %s and year = %s
            """, (fecha,str(years)))    

        if len(con.getCur().fetchall()) > 0 :
            return True
        return False

    def dateExistsFuente(fecha,fuente):
        con.getCur().execute("""
            SELECT * FROM grli_pip_seguimiento_ejecucion_financiera
            WHERE fecha = %s and fuente_financiamiento = %s and anio = %s
            """, (fecha,fuente,str(years)))

        if len(con.getCur().fetchall()) > 0 :
            return True
        return False

    def getultimafecha_grli_pry_mes():
        con.getCur().execute("""
        select max(fecha) as fecha from grli_pry_mes where anio= %s """, (str(years),))

        fecha = con.getCur().fetchone()['fecha']
        if fecha == None:
            return datetime.now().date() + timedelta(days=-1)
            
        return fecha

    def getPrimeraAparicion(codigo, fecha):
        con.getCur().execute("""
            SELECT * FROM grli_pip_seguimiento_ejecucion_financiera
            WHERE anio = %s AND cod_unif = %s and fuente_financiamiento is null
            """, (str(years), codigo) )

        if len(con.getCur().fetchall()) > 0:
            return '0'
        return '1'
    def monto_mes(con,mes):
        con.getCur().execute("""
        select mes,sum(dev_dia) as monto from inf_financiera2 where anio = date_part('year'::text, now()) and 
        fecha =(select max(fecha) from inf_financiera2 where mes = %s) and mes=%s 
        group by mes order by mes """, (str(mes),str(mes)))

        data = con.getCur().fetchone()
        if (data is not None): 
            return data['monto']
        else:
            return 0

    def monto_cambies_mef(con,anio):

        docName_mes = str(datetime.now().date()) + '.html'
        data = reader.bs4Html("./downloads/"+ str(anio) +"/EjecucionPresupuestal/ProyectoMes/" + str(docName_mes))
        trs = data.find("table", class_="Data").contents
        meses = set([])
        for row in trs:
            mes = row.contents[0].text.split(':')[0].strip()
            dev_dia = row.contents[6].text.replace(",", "")
            dev_dia = dev_dia.replace(".", ",")
            dev_dia = Decimal(dev_dia) if len(dev_dia) != 0 else Decimal(0)
            devengado_db = monto_mes(con,(mes))
            if dev_dia > devengado_db + 1:
                meses.add(mes)
            
        mes_c = datetime.now().month
        if mes_c != 1:
            if int(datetime.now().day) == 1 :
                mes_c = mes_c - 1
            else:
                mes_c = mes_c  
        else:
            mes_c=mes_c

        if anio <= 2022:
            mes_c = 12

        meses.add(mes_c)
        return meses


    def insert(data,fecha):
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
            girado_dia = row.contents[7].text.replace(",", "")
            girado_dia = girado_dia.replace(".", ",")
            girado_dia = girado_dia if len(girado_dia) != 0 else 0
            #AVANCE FINANCIERO
            a_financ_dia = row.contents[8].text.replace(",", "")
            a_financ_dia = a_financ_dia if len(a_financ_dia) != 0 else 0
            #a_financ_dia = a_financ_dia.replace(".", ",")
            #Fecha
            fecha = fecha


            camb_pia_dia = '0'
            camb_pim_dia = '0'
            camb_certificacion_dia = '0'
            camb_comp_anual_dia = '0'
            camb_ate_comp_anual_dia = '0'
            camb_dev_dia = '0'
            camb_girado_dia = '0'
            camb_a_financ_dia = '0'
            camb = '0'

            dif_pia_dia = 0.00
            dif_pim_dia = 0.00
            dif_certificacion_dia = 0.00
            dif_comp_anual_dia = 0.00
            dif_ate_comp_anual_dia = 0.00
            dif_dev_dia = 0.00
            dif_girado_dia = 0.00
            dif_a_financ_dia = 0.00

            reg_ant_dia       = '0' #Existe registro anterior?
            primera_aparicion = getPrimeraAparicion(codigo,fecha) #Aparecio en Consulta Amigable - Primera ves
            camb = primera_aparicion

            anio = str(years)

            # Get Ultimo Registro
            ultimoRegistro = getUltimoRegistro(codigo,fecha)

            if ( ultimoRegistro ):

                reg_ant_dia = '1' #Existe registro anterior?

                if( Decimal(ultimoRegistro['pia_dia']) != Decimal(pia_dia) ):
                    camb_pia_dia = '1'
                    dif_pia_dia = Decimal(pia_dia) - Decimal(ultimoRegistro['pia_dia'])
                    #if( codigo == ' 2030921' ):
                        #print 'Actual-> ' + str(Decimal(pia_dia))
                        #print 'Anterior-> ' + str(Decimal(ultimoRegistro['pia_dia']))
                    camb = '1'

                if( Decimal(ultimoRegistro['pim_dia']) != Decimal(pim_dia) ):
                    camb_pim_dia = '1'
                    dif_pim_dia = Decimal(pim_dia) - Decimal(ultimoRegistro['pim_dia'])
                    camb = '1'

                if( Decimal(ultimoRegistro['certificacion_dia']) != Decimal(certificacion_dia) ):
                    camb_certificacion_dia = '1'
                    dif_certificacion_dia = Decimal(certificacion_dia) - Decimal(ultimoRegistro['certificacion_dia'])
                    camb = '1'

                if( Decimal(ultimoRegistro['comp_anual_dia']) != Decimal(comp_anual_dia) ):
                    camb_comp_anual_dia = '1'
                    dif_ate_comp_anual_dia = Decimal(comp_anual_dia) - Decimal(ultimoRegistro['comp_anual_dia'])
                    camb = '1'

                if( Decimal(ultimoRegistro['ate_comp_anual_dia']) != Decimal(ate_comp_anual_dia) ):
                    camb_ate_comp_anual_dia = '1'
                    dif_ate_comp_anual_dia = Decimal(ate_comp_anual_dia) - Decimal(ultimoRegistro['ate_comp_anual_dia'])
                    camb = '1'

                if( Decimal(ultimoRegistro['dev_dia']) != Decimal(dev_dia) ):
                    camb_dev_dia = '1'
                    dif_dev_dia = Decimal(dev_dia) - Decimal(ultimoRegistro['dev_dia'])
                    camb = '1'

                if( Decimal(ultimoRegistro['girado_dia']) != Decimal(girado_dia) ):
                    camb_girado_dia = '1'
                    dif_girado_dia = Decimal(girado_dia) - Decimal(ultimoRegistro['girado_dia'])
                    camb = '1'

                if( Decimal(ultimoRegistro['a_financ_dia']) != Decimal(a_financ_dia) ):
                    camb_a_financ_dia = '1'
                    dif_a_financ_dia = Decimal(a_financ_dia) - Decimal(ultimoRegistro['a_financ_dia'])
                    camb = '1'

            con.getCur().execute("""
                INSERT INTO grli_pip_seguimiento_ejecucion_financiera(
                    cod_unif, /* 0 */
                    pia_dia,  /* 1 */
                    pim_dia,  /* 2 */
                    certificacion_dia, /* 3 */
                    comp_anual_dia, /* 4 */
                    ate_comp_anual_dia, /* 5 */
                    dev_dia, /* 6 */
                    girado_dia, /* 7 */
                    a_financ_dia, /* 8 */
                    fecha, /* 9 */
                    camb_pia_dia, /* 10 */
                    camb_pim_dia, /* 11 */
                    camb_certificacion_dia, /* 12 */
                    camb_comp_anual_dia, /* 13 */
                    camb_ate_comp_anual_dia, /* 13 */
                    camb_dev_dia, /* 14 */
                    camb_girado_dia, /* 15 */
                    camb_a_financ_dia, /* 16 */
                    camb, /* 17 */
                    dif_pia_dia, /* 18 */
                    dif_pim_dia, /* 19 */
                    dif_certificacion_dia, /* 20 */
                    dif_comp_anual_dia, /* 21 */
                    dif_ate_comp_anual_dia, /* 22 */
                    dif_dev_dia, /* 23 */
                    dif_girado_dia, /* 24 */
                    dif_a_financ_dia, /* 25 */
                    reg_ant_dia, /* 26 */
                    primera_aparicion, /* 27 */
                    anio /* 28 */
                    ) VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                    ) """,
                (
                    str(codigo),
                    str(pia_dia),
                    str(pim_dia),
                    str(certificacion_dia),
                    str(comp_anual_dia),
                    str(ate_comp_anual_dia),
                    str(dev_dia),
                    str(girado_dia),
                    str(a_financ_dia),
                    str(fecha),
                    str(camb_pia_dia),
                    str(camb_pim_dia),
                    str(camb_certificacion_dia),
                    str(camb_comp_anual_dia),
                    str(camb_ate_comp_anual_dia),
                    str(camb_dev_dia),
                    str(camb_girado_dia),
                    str(camb_a_financ_dia),
                    str(camb),
                    str(dif_pia_dia),
                    str(dif_pim_dia),
                    str(dif_certificacion_dia),
                    str(dif_comp_anual_dia),
                    str(dif_ate_comp_anual_dia),
                    str(dif_dev_dia),
                    str(dif_girado_dia),
                    str(dif_a_financ_dia),
                    str(reg_ant_dia),
                    str(primera_aparicion),
                    str(anio)
                )
            )

    def insertFuente(data,fecha,fuente):
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
            girado_dia = row.contents[7].text.replace(",", "")
            girado_dia = girado_dia.replace(".", ",")
            girado_dia = girado_dia if len(girado_dia) != 0 else 0
            #AVANCE FINANCIERO
            a_financ_dia = row.contents[8].text.replace(",", "")
            a_financ_dia = a_financ_dia if len(a_financ_dia) != 0 else 0
            #a_financ_dia = a_financ_dia.replace(".", ",")
            #Fecha
            fecha = fecha

            camb_pia_dia = '0'
            camb_pim_dia = '0'
            camb_certificacion_dia = '0'
            camb_comp_anual_dia = '0'
            camb_ate_comp_anual_dia = '0'
            camb_dev_dia = '0'
            camb_girado_dia = '0'
            camb_a_financ_dia = '0'
            camb = '0'

            dif_pia_dia = 0.00
            dif_pim_dia = 0.00
            dif_certificacion_dia = 0.00
            dif_comp_anual_dia = 0.00
            dif_ate_comp_anual_dia = 0.00
            dif_dev_dia = 0.00
            dif_girado_dia = 0.00
            dif_a_financ_dia = 0.00

            reg_ant_dia       = '0' #Existe registro anterior?
            primera_aparicion = getPrimeraAparicion(codigo,fecha) #Aparecio en Consulta Amigable - Primera ves
            camb = primera_aparicion

            anio = str(years)

            # Get Ultimo Regstro
            ultimoRegistro = getUltimoRegistro(codigo,fecha)

            if ( ultimoRegistro ):

                reg_ant_dia = '1' #Existe registro anterior?

                if( Decimal(ultimoRegistro['pia_dia']) != Decimal(pia_dia) ):
                    camb_pia_dia = '1'
                    dif_pia_dia = Decimal(pia_dia) - Decimal(ultimoRegistro['pia_dia'])
                    #if( codigo == ' 2030921' ):
                        #print 'Actual-> ' + str(Decimal(pia_dia))
                        #print 'Anterior-> ' + str(Decimal(ultimoRegistro['pia_dia']))
                    camb = '1'

                if( Decimal(ultimoRegistro['pim_dia']) != Decimal(pim_dia) ):
                    camb_pim_dia = '1'
                    dif_pim_dia = Decimal(pim_dia) - Decimal(ultimoRegistro['pim_dia'])
                    camb = '1'

                if( Decimal(ultimoRegistro['certificacion_dia']) != Decimal(certificacion_dia) ):
                    camb_certificacion_dia = '1'
                    dif_certificacion_dia = Decimal(certificacion_dia) - Decimal(ultimoRegistro['certificacion_dia'])
                    camb = '1'

                if( Decimal(ultimoRegistro['comp_anual_dia']) != Decimal(comp_anual_dia) ):
                    camb_comp_anual_dia = '1'
                    dif_ate_comp_anual_dia = Decimal(comp_anual_dia) - Decimal(ultimoRegistro['comp_anual_dia'])
                    camb = '1'

                if( Decimal(ultimoRegistro['ate_comp_anual_dia']) != Decimal(ate_comp_anual_dia) ):
                    camb_ate_comp_anual_dia = '1'
                    dif_ate_comp_anual_dia = Decimal(ate_comp_anual_dia) - Decimal(ultimoRegistro['ate_comp_anual_dia'])
                    camb = '1'

                if( Decimal(ultimoRegistro['dev_dia']) != Decimal(dev_dia) ):
                    camb_dev_dia = '1'
                    dif_dev_dia = Decimal(dev_dia) - Decimal(ultimoRegistro['dev_dia'])
                    camb = '1'

                if( Decimal(ultimoRegistro['girado_dia']) != Decimal(girado_dia) ):
                    camb_girado_dia = '1'
                    dif_girado_dia = Decimal(girado_dia) - Decimal(ultimoRegistro['girado_dia'])
                    camb = '1'

                if( Decimal(ultimoRegistro['a_financ_dia']) != Decimal(a_financ_dia) ):
                    camb_a_financ_dia = '1'
                    dif_a_financ_dia = Decimal(a_financ_dia) - Decimal(ultimoRegistro['a_financ_dia'])
                    camb = '1'

            con.getCur().execute("""
                INSERT INTO grli_pip_seguimiento_ejecucion_financiera(
                    cod_unif, /* 0 */
                    pia_dia,  /* 1 */
                    pim_dia,  /* 2 */
                    certificacion_dia, /* 3 */
                    comp_anual_dia, /* 4 */
                    ate_comp_anual_dia, /* 5 */
                    dev_dia, /* 6 */
                    girado_dia, /* 7 */
                    a_financ_dia, /* 8 */
                    fecha, /* 9 */
                    camb_pia_dia, /* 10 */
                    camb_pim_dia, /* 11 */
                    camb_certificacion_dia, /* 12 */
                    camb_comp_anual_dia, /* 13 */
                    camb_ate_comp_anual_dia, /* 13 */
                    camb_dev_dia, /* 14 */
                    camb_girado_dia, /* 15 */
                    camb_a_financ_dia, /* 16 */
                    camb, /* 17 */
                    dif_pia_dia, /* 18 */
                    dif_pim_dia, /* 19 */
                    dif_certificacion_dia, /* 20 */
                    dif_comp_anual_dia, /* 21 */
                    dif_ate_comp_anual_dia, /* 22 */
                    dif_dev_dia, /* 23 */
                    dif_girado_dia, /* 24 */
                    dif_a_financ_dia, /* 25 */
                    reg_ant_dia, /* 26 */
                    primera_aparicion, /* 27 */
                    anio, /* 28 */
                    fuente_financiamiento /* 29 */
                    ) VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                    ) """,
                (
                    str(codigo),
                    str(pia_dia),
                    str(pim_dia),
                    str(certificacion_dia),
                    str(comp_anual_dia),
                    str(ate_comp_anual_dia),
                    str(dev_dia),
                    str(girado_dia),
                    str(a_financ_dia),
                    str(fecha),
                    str(camb_pia_dia),
                    str(camb_pim_dia),
                    str(camb_certificacion_dia),
                    str(camb_comp_anual_dia),
                    str(camb_ate_comp_anual_dia),
                    str(camb_dev_dia),
                    str(camb_girado_dia),
                    str(camb_a_financ_dia),
                    str(camb),
                    str(dif_pia_dia),
                    str(dif_pim_dia),
                    str(dif_certificacion_dia),
                    str(dif_comp_anual_dia),
                    str(dif_ate_comp_anual_dia),
                    str(dif_dev_dia),
                    str(dif_girado_dia),
                    str(dif_a_financ_dia),
                    str(reg_ant_dia),
                    str(primera_aparicion),
                    str(anio),
                    str(fuente)
                )
            )
            con.getConn().commit()


    def insertFormato12b(data,fecha):
        lista_f12b =  lista_formato_12b()
        for row in data:
            codigo = row['cod_unif']
            if int(codigo) not in  lista_f12b:
                print("CONSULTANDO FORMATO 12-B")

                # #Nombre Proyecto
                # nombreProyecto = row.contents[0].text
                # #Codigo Unificado del proyecto
                # codigo = nombreProyecto.split(':')[0].strip()
                print ('FORMATO ['+str(codigo)+'] -> Tabla [formato12b]...')
                mef = requester.requestMEF(codigo)
                
                if mef:
                    formato12b = mef[0]   
                    #### Datos Formato 12B
                    _fech_act         = None
                    _exp_reg          = "NO" 
                    _pro_ene          = 0.00
                    _pro_feb          = 0.00
                    _pro_mar          = 0.00
                    _pro_abr          = 0.00
                    _pro_may          = 0.00
                    _pro_jun          = 0.00
                    _pro_jul          = 0.00
                    _pro_ago          = 0.00
                    _pro_set          = 0.00
                    _pro_oct          = 0.00
                    _pro_nov          = 0.00
                    _pro_dic          = 0.00
                    _act_ene          = 0.00
                    _act_feb          = 0.00
                    _act_mar          = 0.00
                    _act_abr          = 0.00
                    _act_may          = 0.00
                    _act_jun          = 0.00
                    _act_jul          = 0.00
                    _act_ago          = 0.00
                    _act_set          = 0.00
                    _act_oct          = 0.00
                    _act_nov          = 0.00
                    _act_dic          = 0.00
                    _dev_ene          = 0.00
                    _dev_feb          = 0.00
                    _dev_mar          = 0.00
                    _dev_abr          = 0.00
                    _dev_may          = 0.00
                    _dev_jun          = 0.00
                    _dev_jul          = 0.00
                    _dev_ago          = 0.00
                    _dev_set          = 0.00
                    _dev_oct          = 0.00
                    _dev_nov          = 0.00
                    _dev_dic          = 0.00
                    _total_pro        = 0.00
                    _total_act        = 0.00
                    _total_dev        = 0.00
                    _fecha_et         = None
                    _tipo_formato     = ""
                    _modal_ejec       = ""
                    _avance_fisico    = 0.00
                    _ult_est_situal   = ""
                    _ult_periodo_reg_f12b =""
                    _fec_ini_ejec    = None
                    _fec_fin_ejec    = None
                    _avance_ejecucion= 0.00
                    _cierre          = ""
                    _fec_declara_estim    = None
                    _costo_actualizado = 0.00
                    _devengado_acumulado =  0.00

                    #LLENANDO DATOS
                    _fech_act         = formato12b['FECHA_ULT_ACT_F12B']
                    _pro_ene          = formato12b['MONTO_PROGRAMADO_1']
                    _pro_feb          = formato12b['MONTO_PROGRAMADO_2']
                    _pro_mar          = formato12b['MONTO_PROGRAMADO_3']
                    _pro_abr          = formato12b['MONTO_PROGRAMADO_4']
                    _pro_may          = formato12b['MONTO_PROGRAMADO_5']
                    _pro_jun          = formato12b['MONTO_PROGRAMADO_6']
                    _pro_jul          = formato12b['MONTO_PROGRAMADO_7']
                    _pro_ago          = formato12b['MONTO_PROGRAMADO_8']
                    _pro_set          = formato12b['MONTO_PROGRAMADO_9']
                    _pro_oct          = formato12b['MONTO_PROGRAMADO_10']
                    _pro_nov          = formato12b['MONTO_PROGRAMADO_11']
                    _pro_dic          = formato12b['MONTO_PROGRAMADO_12']
                    _act_ene          = formato12b['MONTO_ACTUALIZADO_1']
                    _act_feb          = formato12b['MONTO_ACTUALIZADO_2']
                    _act_mar          = formato12b['MONTO_ACTUALIZADO_3']
                    _act_abr          = formato12b['MONTO_ACTUALIZADO_4']
                    _act_may          = formato12b['MONTO_ACTUALIZADO_5']
                    _act_jun          = formato12b['MONTO_ACTUALIZADO_6']
                    _act_jul          = formato12b['MONTO_ACTUALIZADO_7']
                    _act_ago          = formato12b['MONTO_ACTUALIZADO_8']
                    _act_set          = formato12b['MONTO_ACTUALIZADO_9']
                    _act_oct          = formato12b['MONTO_ACTUALIZADO_10']
                    _act_nov          = formato12b['MONTO_ACTUALIZADO_11']
                    _act_dic          = formato12b['MONTO_ACTUALIZADO_12']
                    _dev_ene          = formato12b['DEV_ANO_VIG_1']
                    _dev_feb          = formato12b['DEV_ANO_VIG_2']
                    _dev_mar          = formato12b['DEV_ANO_VIG_3']
                    _dev_abr          = formato12b['DEV_ANO_VIG_4']
                    _dev_may          = formato12b['DEV_ANO_VIG_5']
                    _dev_jun          = formato12b['DEV_ANO_VIG_6']
                    _dev_jul          = formato12b['DEV_ANO_VIG_7']
                    _dev_ago          = formato12b['DEV_ANO_VIG_8']
                    _dev_set          = formato12b['DEV_ANO_VIG_9']
                    _dev_oct          = formato12b['DEV_ANO_VIG_10']
                    _dev_nov          = formato12b['DEV_ANO_VIG_11']
                    _dev_dic          = formato12b['DEV_ANO_VIG_12']
                    _fecha_et         = formato12b['FECHA_ET']
                    _tipo_formato     = formato12b['TIPO_FORMATO']
                    _modal_ejec       = formato12b['MODAL_EJEC']
                    _avance_fisico    = formato12b['PORC_AVANCE_EJEC']
                    _ult_est_situal   = formato12b['ULT_ESTADO_SITUACIONAL']
                    _ult_periodo_reg_f12b = formato12b['ULT_PERIODO_REG_F12B']
                    _fec_ini_ejec = formato12b['FEC_INI_EJ']
                    _fec_fin_ejec = formato12b['FEC_FIN_EJ']
                    _avance_ejecucion = formato12b['PORC_AVANCE_FIS']
                    _cierre           = formato12b['CIERRE_REGISTRADO']
                    _fec_declara_estim    = formato12b['FEC_DECLARA_ESTIM']
                    _costo_actualizado    = formato12b['COSTO_ACTUALIZADO']
                    _devengado_acumulado    = formato12b['DEV_ACUMULADO']
                                                        
                    _total_pro  =  _pro_ene + _pro_feb + _pro_mar + _pro_abr + _pro_may + _pro_jun + _pro_jul + _pro_ago + _pro_set + _pro_oct + _pro_nov + _pro_dic
                    _total_act  =  _act_ene + _act_feb + _act_mar + _act_abr + _act_may + _act_jun + _act_jul + _act_ago + _act_set + _act_oct + _act_nov + _act_dic
                    _total_dev  =  _dev_ene + _dev_feb + _dev_mar + _dev_abr + _dev_may + _dev_jun + _dev_jul + _dev_ago + _dev_set + _dev_oct + _dev_nov + _dev_dic

                    if  _fech_act != None :
                        _fech_act = _fech_act.split('(')
                        _fech_act = _fech_act[1].split(')')
                        _fech_act = int(_fech_act[0])/1000
                        _fech_act = datetime.fromtimestamp(_fech_act).strftime('%Y-%m-%d')   

                    if  _fecha_et != None :
                        _fecha_et = _fecha_et.split('(')
                        _fecha_et = _fecha_et[1].split(')')
                        _fecha_et = int(_fecha_et[0])/1000
                        _fecha_et = datetime.fromtimestamp(_fecha_et).strftime('%Y-%m-%d')             

                    if _total_pro != 0 or _total_act != 0:
                        _exp_reg = "SI"

                    if _tipo_formato == "IOARR":
                        _tipo_formato = requester.requestIOARREmergencia(codigo)

                    #Para grli_pip_total_priori
                    if _tipo_formato is not None:
                        if _tipo_formato == 'PROYECTO DE INVERSION':
                            _tipo_proyecto = 'PIP'
                            _tipo_pry = 'PIP'
                        elif _tipo_formato == 'IOARR':
                            _tipo_proyecto = _tipo_formato
                            _tipo_pry = 'NO PIP'
                        elif _tipo_formato == 'FUR':
                            _tipo_proyecto = 'RCC'
                            _tipo_pry = 'RCC'

                    # con.getCur().execute("""
                    #     UPDATE grli_pip_total_priori SET
                    #         tipo_inversion = %s,
                    #         tipo_pry = %s,
                    #         cierre = %s,
                    #         formato12b = %s
                    #     WHERE cod_unif = %s
                    # """, (
                    #         _tipo_proyecto,
                    #         _tipo_pry,
                    #         _cierre,
                    #         'SI',
                    #         codigo
                    # ))

                    con.getCur().execute("""
                        INSERT INTO formato12b(
                            cod_unif,
                            exp_reg,
                            fecha_actual,
                            year,
                            p_enero,
                            p_febrero,
                            p_marzo,
                            p_abril,
                            p_mayo,
                            p_junio,
                            p_julio,
                            p_agosto,
                            p_setiembre,
                            p_octubre,
                            p_noviembre,
                            p_diciembre,
                            a_enero,
                            a_febrero,
                            a_marzo,
                            a_abril,
                            a_mayo,
                            a_junio,
                            a_julio,
                            a_agosto,
                            a_setiembre,
                            a_octubre,
                            a_noviembre,
                            a_diciembre,
                            fecha_subida,
                            fecha_et,     
                            tipo_formato,
                            modal_ejec,
                            avance_fisico,
                            ult_est_situal,
                            ult_periodo_reg_f12b,
                            fec_ini_ejec,
                            fec_fin_ejec,
                            avance_ejecucion,
                            cierre,
                            dev_enero,
                            dev_febrero,
                            dev_marzo,
                            dev_abril,
                            dev_mayo,
                            dev_junio,
                            dev_julio,
                            dev_agosto,
                            dev_setiembre,
                            dev_octubre,
                            dev_noviembre,
                            dev_diciembre,
                            total_programado,
                            total_actualizado,
                            total_devengado,
                            fec_declara_estim,
                            costo_actualizado,
                            devengado_acumulado
                            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                            (codigo,_exp_reg,_fech_act,years,_pro_ene,_pro_feb,_pro_mar,_pro_abr,_pro_may,_pro_jun,_pro_jul,_pro_ago,_pro_set,_pro_oct,_pro_nov,
                            _pro_dic,_act_ene,_act_feb,_act_mar,_act_abr,_act_may,_act_jun,_act_jul,_act_ago,_act_set,_act_oct,_act_nov,_act_dic,None,_fecha_et,     
                            _tipo_formato,_modal_ejec,_avance_fisico,_ult_est_situal,_ult_periodo_reg_f12b,_fec_ini_ejec,_fec_fin_ejec,_avance_ejecucion,_cierre,
                            _dev_ene,_dev_feb,_dev_mar,_dev_abr,_dev_may,_dev_jun,_dev_jul,_dev_ago,_dev_set,_dev_oct,_dev_nov,_dev_dic,_total_pro,_total_act,_total_dev,
                            _fec_declara_estim,_costo_actualizado,_devengado_acumulado
                    ))
                    print ('GUARDANDO FORMATO ['+str(codigo)+'] -> Tabla [formato12b]...')
                else:
                    con.getCur().execute("""
                        UPDATE grli_pip_total_priori SET
                            formato12b = %s
                        WHERE cod_unif = %s
                    """, (
                            'NO',
                            codigo
                    ))
                    print ("NO HAY INFORMACION DEL FORMATO 12-B => "+ str(codigo))
            else:
                print("YA EXISTE EN LA BASE DE DATOS")
            con.getConn().commit()


    def insertDevMensualisado(data,fecha,mes):
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
            girado_dia = row.contents[7].text.replace(",", "")
            girado_dia = girado_dia.replace(".", ",")
            girado_dia = girado_dia if len(girado_dia) != 0 else 0
            #AVANCE FINANCIERO
            a_financ_dia = row.contents[8].text.replace(",", "")
            a_financ_dia = a_financ_dia if len(a_financ_dia) != 0 else 0
            #a_financ_dia = a_financ_dia.replace(".", ",")
            #Fecha
            fecha = fecha

            con.getCur().execute("""
                INSERT INTO inf_financiera2(
                    codigo,
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
                    mes
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """,
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
                    str(mes)
                )
            )
            con.getConn().commit()
    
    def insert_grli_pry_mes(fecha,mes):
        print("------------ Cargando datos para ingresar a grli_pry_mes--------------------")
        print(str(mes))
        fechaultregistro = datetime.strptime(str(getultimafecha_grli_pry_mes()), '%Y-%m-%d').date()
        files = os.listdir('./downloads/'+ str(years) +'/EjecucionPresupuestal/ProyectoMes/' + str(mes))
        file = max(files)
        data = reader.bs4Html("./downloads/"+ str(years) +"/EjecucionPresupuestal/ProyectoMes/"  + str(mes) + "/" + str(file))
        trs = data.find("table", class_="Data").contents
        if trs:
            print("gf")
            if fecha > fechaultregistro:
                print("gf")
                con.getCur().execute(""" select * from sp_grli_pry_mes(%s)""",(years,) )
                data = con.getCur().fetchall()
                print(data)
                if data:
                    for row in data:
                        enero = row['enero'] if row['enero'] is not None else 0
                        febrero = row['febrero'] if row['febrero'] is not None else 0
                        marzo = row['marzo'] if row['marzo'] is not None else 0
                        abril = row['abril'] if row['abril'] is not None else 0
                        mayo = row['mayo'] if row['mayo'] is not None else 0
                        junio = row['junio'] if row['junio'] is not None else 0
                        julio = row['julio'] if row['julio'] is not None else 0
                        agosto = row['agosto'] if row['agosto'] is not None else 0
                        septiembre = row['septiembre'] if row['septiembre'] is not None else 0
                        octubre = row['octubre'] if row['octubre'] is not None else 0
                        noviembre = row['noviembre'] if row['noviembre'] is not None else 0
                        diciembre = row['diciembre'] if row['diciembre'] is not None else 0
                        fecha = row['fecha'] if row['fecha'] is not None else fecha
                        enero_certificado = row['enero_certificado'] if row['enero_certificado'] is not None else 0
                        febrero_certificado = row['febrero_certificado'] if row['febrero_certificado'] is not None else 0
                        marzo_certificado = row['marzo_certificado'] if row['marzo_certificado'] is not None else 0
                        abril_certificado = row['abril_certificado'] if row['abril_certificado'] is not None else 0
                        mayo_certificado = row['mayo_certificado'] if row['mayo_certificado'] is not None else 0
                        junio_certificado = row['junio_certificado'] if row['junio_certificado'] is not None else 0
                        julio_certificado = row['julio_certificado'] if row['julio_certificado'] is not None else 0
                        agosto_certificado = row['agosto_certificado'] if row['agosto_certificado'] is not None else 0
                        septiembre_certificado = row['septiembre_certificado'] if row['septiembre_certificado'] is not None else 0
                        octubre_certificado = row['octubre_certificado'] if row['octubre_certificado'] is not None else 0
                        noviembre_certificado = row['noviembre_certificado'] if row['noviembre_certificado'] is not None else 0
                        diciembre_certificado = row['diciembre_certificado'] if row['diciembre_certificado'] is not None else 0
                        con.getCur().execute("""
                            INSERT INTO grli_pry_mes(
                                cod_unif,
                                enero,
                                febrero,
                                marzo,
                                abril,
                                mayo,
                                junio,
                                julio,
                                agosto,
                                septiembre,
                                octubre,
                                noviembre,
                                diciembre,
                                fecha,
                                anio,
                                enero_certificado,
                                febrero_certificado,
                                marzo_certificado,
                                abril_certificado,
                                mayo_certificado,
                                junio_certificado,
                                julio_certificado,
                                agosto_certificado,
                                septiembre_certificado,
                                octubre_certificado,
                                noviembre_certificado,
                                diciembre_certificado
                                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """,
                            (
                                str(row['cod_unif']),
                                str(enero),
                                str(febrero),
                                str(marzo),
                                str(abril),
                                str(mayo),
                                str(junio),
                                str(julio),
                                str(agosto),
                                str(septiembre),
                                str(octubre),
                                str(noviembre),
                                str(diciembre),
                                str(fecha),
                                str(years),
                                str(enero_certificado),
                                str(febrero_certificado),
                                str(marzo_certificado),
                                str(abril_certificado),
                                str(mayo_certificado),
                                str(junio_certificado),
                                str(julio_certificado),
                                str(agosto_certificado),
                                str(septiembre_certificado),
                                str(octubre_certificado),
                                str(noviembre_certificado),
                                str(diciembre_certificado)
                            )
                        )
                    con.getConn().commit()
                    print("------------ Datos ingresados a grli_pry_mes  con fecha " + str(fecha) + "--------------------")
                else:
                    print("------------ SIN DATA -------------------")
            else :
                print("------------ Datos ya ingresados a grli_pry_mes con fecha " + str(fecha) + "--------------------")
        else:
            print("------------ Datos " + str(mes) + " no contiene datos con fecha " + str(fecha) + "--------------------")

    def __init__():
        print('INICIANDO...')
        files = os.listdir('./downloads/'+ str(years) +'/EjecucionPresupuestal/Proyectos')
        fechaultregistro = datetime.strptime(str(getultimafecharegistro()), '%Y-%m-%d').date()
        #GUARDAR LOS PROYECTOS 
        print ("INGRESANDO PROYECTOS DE "+ str(fechaultregistro) +" A "+str(datetime.now().date()))
        for file in files:
            fecha = datetime.strptime(file.split('.')[0], '%Y-%m-%d').date()
            if fecha > fechaultregistro:
                print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ACTUALIZANDO A LA FECHA => " + str(fecha) + " Tabla [grli_pip_seguimiento_ejecucion_financiera] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
                #PARA PROYECTOS
                data = reader.bs4Html("./downloads/"+ str(years) +"/EjecucionPresupuestal/Proyectos/" + str(file))
                print("Processing file " + str(file))
                trs = data.find("table", class_="Data").contents
                insert( trs, file.split(".")[0])
                print("------> PROYECTOS INGRESADOS")
                #PARA PROYECTOS FUENTE RECURSOS ORDINARIOS
                data_RO = reader.bs4Html("./downloads/"+ str(years) +"/EjecucionPresupuestal/RECURSOS_ORDINARIOS/"+str(file))
                trs_RO = data_RO.find('table', class_="Data").contents
                insertFuente(trs_RO, file.split('.')[0],"RECURSOS ORDINARIOS")
                print("------> PROYECTOS INGRESADOS [FUENTE DE RECURSOS ORDINARIOS]")
                #PARA PROYECTOS FUENTE RECURSOS POR OPERACIONES OFICIALES DE CREDITO
                data_ROC = reader.bs4Html("./downloads/"+ str(years) +"/EjecucionPresupuestal/RECURSOS_POR_OPERACIONES_OFICIALES_DE_CREDITO/"+str(file))
                trs_ROC = data_ROC.find('table', class_="Data").contents
                insertFuente(trs_ROC, file.split('.')[0],"RECURSOS POR OPERACIONES OFICIALES DE CREDITO")
                print("------> PROYECTOS INGRESADOS [FUENTE DE RECURSOS POR OPERACIONES OFICIALES DE CREDITO]")
                #PARA PROYECTOS FUENTE RECURSOS DETERMINADOS
                data_RD = reader.bs4Html("./downloads/"+ str(years) +"/EjecucionPresupuestal/RECURSOS_DETERMINADOS/"+str(file))
                trs_RD = data_RD.find('table', class_="Data").contents
                insertFuente(trs_RD, file.split('.')[0],"RECURSOS DETERMINADOS")
                print("------> PROYECTOS INGRESADOS [FUENTE DE RECURSOS DETERMINADOS]")
                #PARA PROYECTOS FUENTE DONACIONES Y TRANSFERENCIAS
                data_DT = reader.bs4Html("./downloads/"+ str(years) +"/EjecucionPresupuestal/DONACIONES_TRANSFERENCIAS/"+str(file))
                trs_DT = data_DT.find('table', class_="Data").contents
                insertFuente(trs_DT, file.split('.')[0],"DONACIONES Y TRANSFERENCIAS")
                print("------> PROYECTOS INGRESADOS [FUENTE DE DONACIONES Y TRANSFERENCIAS]")
                #RECURSOS_DIRECTAMENTE_RECAUDADOS
                data_DT = reader.bs4Html("./downloads/"+ str(years) +"/EjecucionPresupuestal/RECURSOS_DIRECTAMENTE_RECAUDADOS/"+str(file))
                trs_DT = data_DT.find('table', class_="Data").contents
                insertFuente(trs_DT, file.split('.')[0],"RECURSOS DIRECTAMENTE RECAUDADOS")
                print("------> PROYECTOS INGRESADOS [FUENTE DE RECURSOS DIRECTAMENTE RECAUDADOS]")
            else:
                print("DATOS YA ACTUALIZADOS " + str(fecha) + " Tabla [grli_pip_seguimiento_ejecucion_financiera]...")

        meses = monto_cambies_mef(con,years)
        print(meses)
        for mes in meses:
            print('INICIANDO POR MES DEVENGADO... -> ' + str(mes))
            files = os.listdir('./downloads/'+ str(years) +'/EjecucionPresupuestal/ProyectoMes/' + str(mes))
            fechaultregistro = datetime.strptime(str(getultimafecharegistro_mensualisado(mes)), '%Y-%m-%d').date()
            print ("INGRESANDO MES DE [" + str(mes) + "] " + str(fechaultregistro) +" A "+str(datetime.now().date()))
            for file in files:
                fecha = datetime.strptime(file.split('.')[0], '%Y-%m-%d').date()
                if fecha > fechaultregistro:
                    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ACTUALIZANDO A LA FECHA => " + str(fecha) + " Tabla [inf_financiera2 por  MES {" + str(mes) + "}] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
                    data = reader.bs4Html("./downloads/"+ str(years) +"/EjecucionPresupuestal/ProyectoMes/"  + str(mes) + "/" + str(file))
                    trs = data.find("table", class_="Data").contents
                    insertDevMensualisado( trs, file.split(".")[0],str(mes))
                    print("------> POR MES INGRESADOS DEVENGADO -> " + str(mes))
                    
                else:
                    print("DATOS YA ACTUALIZADOS " + str(fecha) + " Tabla [inf_financiera2 por MES {" + str(mes) + "}]...")
        # con.getConn().commit()

        # if years == datetime.now().year :
        #     #GUARDANDO FORMATO 12-B
        #     file = max(files)
        #     fechahoy = datetime.now().date()
        #     print ("INGRESANDO FORMATO 12-B ==> " +str(fechahoy))
        #     if not dateExists_Formato12b(fechahoy):
        #         print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> FORMATO 12-B ACTUALIZANDOS A LA FECHA => " + str(fechahoy) + " Tabla [formato12b] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        #         #PARA PROYECTOS
        #         data_F12B = reader.bs4Html("./downloads/"+ str(years) +"/EjecucionPresupuestal/Proyectos/" + str(fechahoy) + ".html")
        #         trs_F12B = data_F12B.find("table", class_="Data").contents
        #         insertFormato12b(trs_F12B, file.split('.')[0])
        #         print("------> PROYECTOS INGRESADOS AL FORMATO 12-B")
        #     else:
        #         print("DATOS YA ACTUALIZADOS DEL FORMATO 12-B " + str(fechahoy) + " Tabla [formato12b]...")   
        # Ingresar datos a la tabla grli_pry_mes
        insert_grli_pry_mes(fecha,mes)
        fechahoy = datetime.now().date()
       
        # ruta  = './downloads/' + str(years) + '/EjecucionPresupuestal/ProyectosJSON/DataInv.json' 
        # data = None
        # with open(ruta, "r") as file:
        #     data = json.load(file)
            
        
        # # print(getformato12b())
        # dataRow = []
        
        # for rows in data:
        #     dataRow.append(rows['CODIGO_UNICO'])

        # data_ext = []
        # [data_ext.append(row) for row in dataRow if row not in data_ext]
        # data_int = []
        # [data_int.append(row) for row in  getformato12b()]
        # data_int = getformato12b()

        # print(data_int)
 

        # print ("INGRESANDO FORMATO 12-B ==> " +str(fechahoy))
        # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> FORMATO 12-B ACTUALIZADOS A LA FECHA => " + str(fechahoy) + " Tabla [formato12b] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
       
        # if len(getformato12b()) == len(lista_formato_12b()) :
        #     print("ACTUALIZANDO FECHA A " + str(fechahoy) )
        #     con.getCur().execute("""
        #                 UPDATE formato12b SET
        #                     fecha_subida = %s
        #                 WHERE fecha_subida is null
        #             """, (
        #                     fechahoy,
        #             ))
        #     con.getConn().commit()
        # else:
        #     print(len(getformato12b()))
        #     print(len(lista_formato_12b()))
        #     insertFormato12b(getformato12b(),fechahoy)
        # print("------> PROYECTOS INGRESADOS AL FORMATO 12-B")
        

    # Iniciar
    __init__()
