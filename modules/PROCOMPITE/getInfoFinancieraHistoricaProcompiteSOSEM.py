import sys
import requester
import os
from decimal import Decimal
import reader
from commons import twoDec
from importlib import reload
from datetime import datetime
import urllib.request as urllib2
import json
import decimal
from decimal import *
import time

reload(sys)

def main(con,years):
    now = datetime.now()
    # FUNCTIONS
    def flagSiNo(str):
        if str == "1":
            return "Si"
        elif str == "0":
            return "No"
        else:
            return "---"

    def oneDec(num):
        num = Decimal(num).quantize(Decimal('1.00'))
        return num  

    unidadesEjecutoras = {
        1027: 'REGION LIMA',
        1325: 'REGION LIMA - SUB GERENCIA REGIONAL LIMA SUR',
        1228: 'REGION LIMA - DIRECCIÓN REGIONAL DE AGRICULTURA LIMA PROVINCIAS',
        1190: 'REGION LIMA - DIRECCIÓN REGIONAL DE EDUCACION LIMA	PROVINCIAS',
        1181: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL CAÑETE',
        1182: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL HUAURA',
        1183: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL HUARAL',
        1184: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL CAJATAMBO',
        1185: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL CANTA',
        1186: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL YAUYOS',
        1187: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL OYON',
        1188: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL HUAROCHIRI',
        1189: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL BARRANCA',
        1285: 'REGION LIMA - DIRECCION DE SALUD III	LIMA NORTE',
        1286: 'REGION LIMA - HOSP.HUACHO - HUAURA - OYON Y SERV.BASICOS DE SALUD',
        1287: 'REGION LIMA - SERVICIOS BASICOS DE SALUD	CAÑETE - YAUYOS',
        1288: 'REGION LIMA - HOSPITAL DE APOYO REZOLA',
        1289: 'REGION LIMA - HOSP.BARRANCA - CAJATAMBO Y SERV.BASICOS DE SALUD',
        1290: 'REGION LIMA - HOSP.CHANCAY Y SERVICIOS BASICOS DE SALUD',
        1291: 'REGION LIMA - SERV.BASICOS DE SALUD CHILCA - MALA',
        1292: 'REGION LIMA - HOSPITAL HUARAL Y SERVICIOS BASICOS DE	SALUD',
        1404: 'GOB.REG.DE LIMA - RED DE	SALUD DE HUAROCHIRÍ'
    }
    
    def getProjects():
        print("OBTENIENDO DATOS DE TB --> [grli_pip_procompite]")
        con.getCur().execute("""SELECT grli_pip_procompite.cod_unif, grli_pip_procompite.id FROM grli_pip_procompite
              inner join vw_grli_pip_seguimiento_ejecucion_financiera   on grli_pip_procompite.cod_unif =vw_grli_pip_seguimiento_ejecucion_financiera.cod_unif""")
        return con.getCur().fetchall()

    def updateWithInfoSiafFinanciera(infoSiafdata, codigoUnif, idProyecto):
        for infoSiaf in infoSiafdata:
            _m_pim_a_todas_ue    = Decimal(0.00)
            _m_deveng_a_todas_ue = Decimal(0.00)
            _a_financ_a          = Decimal(0.00)
            _tipo_proyecto       = infoSiaf['TIPO_FORMATO']
            ### Info Financiera
            
            fecha = str(datetime.now().date())
            infoSSI = requester.requestSSI(codigoUnif,'UEPA',20)
            infoFTE = requester.requestSSI(codigoUnif,'FTE',19)
            infoMES = requester.requestSSI(codigoUnif,'UEPM',20)

            if infoSSI:
                con.getCur().execute(""" DELETE FROM inf_financiera WHERE cod_unif = %s """,(codigoUnif,))
                for e in infoSSI:
                    #### Datos financieros
                    _uni_ejec         = ""
                    _anio_financ      = ""
                    _pia              = 0.00
                    _pim              = 0.00
                    _dev              = 0.00
                    _comp_anua        = 0.00
                    _certi            = 0.00
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
                    _fuente_financ    = ""

                    _m_pim_a_todas_ue += twoDec(e['MTO_PIM'])
                    _m_deveng_a_todas_ue += twoDec(e['MTO_DEVEN'])

                    if int(e['SEC_EJEC']) in unidadesEjecutoras:
                        #### Fuente de Financieamiento
                        if infoFTE: 
                            for f in infoFTE:
                                if f['NUM_ANIO'] == e['NUM_ANIO']:
                                    _fuente_financ = str(f['DES_FUENTE_FINANC'])
                                    break
                        else:
                            print ('No hay Fuentes de financiamiento')

                        _uni_ejec      = unidadesEjecutoras[int(e['SEC_EJEC'])]
                        _cod_uni_ejec  = int(e['SEC_EJEC'])
                        _anio_financ   = e['NUM_ANIO']
                        _pia           = str(e['MTO_PIA'])
                        _pim           = str(e['MTO_PIM'])
                        _dev           = str(e['MTO_DEVEN'])
                        _comp_anual    = str(e['MTO_COMPROM'])
                        _certif        = str(e['MTO_CERT'])
                        

                        if infoMES:
                            array_mes = {
                                1: 0.00,
                                2: 0.00,
                                3: 0.00,
                                4: 0.00,
                                5: 0.00,
                                6: 0.00,
                                7: 0.00,
                                8: 0.00,
                                9: 0.00,
                                10: 0.00,
                                11: 0.00,
                                12: 0.00,
                            }
                            for f_mes in infoMES:
                                if _cod_uni_ejec ==  f_mes['SEC_EJEC'] and _anio_financ == f_mes['NUM_ANIO'] :
                                    mes = f_mes['COD_MES']
                                    devengado_mensual = f_mes['MTO_DEVEN']
                                    array_mes[mes] = devengado_mensual

                            _dev_ene          = array_mes[1]
                            _dev_feb          = array_mes[2]
                            _dev_mar          = array_mes[3]
                            _dev_abr          = array_mes[4]
                            _dev_may          = array_mes[5]
                            _dev_jun          = array_mes[6]
                            _dev_jul          = array_mes[7]
                            _dev_ago          = array_mes[8]
                            _dev_set          = array_mes[9]
                            _dev_oct          = array_mes[10]
                            _dev_nov          = array_mes[11]
                            _dev_dic          = array_mes[12]

                            
                        print (' ________________________________________________________')
                        print ('|UNIDAD EJECUTORA : '         + str(_uni_ejec))
                        print ('|Año :              '         + str(_anio_financ))
                        print ('|_Codigo Ejecutora       -> ' + str(e['SEC_EJEC']))
                        print ('|_Fuente Financiamiento  -> ' + str(_fuente_financ))
                        print ('|_Pia                    -> ' + str(_pia))
                        print ('|_Pim                    -> ' + str(_pim))
                        print ('|_Dev                    -> ' + str(_dev))
                        print ('|_Compromiso Anual       -> ' + str(_comp_anual))
                        print ('|_Certificado            -> ' + str(_certif))
                        print ('|_Devengado Enero        -> ' + str(_dev_ene))
                        print ('|_Devengado Febrero      -> ' + str(_dev_feb))
                        print ('|_Devengado Marzo        -> ' + str(_dev_mar))
                        print ('|_Devengado Abril        -> ' + str(_dev_abr))
                        print ('|_Devengado Mayo         -> ' + str(_dev_may))
                        print ('|_Devengado Junio        -> ' + str(_dev_jun))
                        print ('|_Devengado Julio        -> ' + str(_dev_jul))
                        print ('|_Devengado Agosto       -> ' + str(_dev_ago))
                        print ('|_Devengado Septiembre   -> ' + str(_dev_set))
                        print ('|_Devengado Octubre      -> ' + str(_dev_oct))
                        print ('|_Devengado Noviembre    -> ' + str(_dev_nov))
                        print ('|_Devengado Diciembre    -> ' + str(_dev_dic))
                        print ('|________________________________________________________')

                        con.getCur().execute("""
                            INSERT INTO inf_financiera (
                                cod_unif,
                                uni_ejec,
                                anio_financ,
                                fuente_financ,
                                pia,
                                dev_ene,
                                dev_feb,
                                dev_mar,
                                dev_abr,
                                dev_may,
                                dev_jun,
                                dev_jul,
                                dev_ago,
                                dev_set,
                                dev_oct,
                                dev_nov,
                                dev_dic,
                                dev,
                                pim,
                                comp_anual,
                                certif,fecha_act) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                                (
                                codigoUnif,_uni_ejec,_anio_financ,_fuente_financ,_pia,_dev_ene,
                                _dev_feb,_dev_mar,_dev_abr,_dev_may,_dev_jun,_dev_jul,
                                _dev_ago,_dev_set,_dev_oct,_dev_nov,_dev_dic,_dev,_pim,
                                _comp_anual,_certif,fecha
                        ))


            con.getConn().commit()  

    def __init__():
        
        projects = getProjects()

        for i in projects:

            print ('********************************************************************************************* CODIGO SIAF <<' + i['cod_unif'] + '>> AÑO ' +  ' PARA TABLA INF_FINANCIERA(PEOCOMPITE) '  + '**********************************************************************************************')
            print ('ID -> ' + str(i['id']))
            # VARS
            codigoUnif, idProyecto, flagSnip =  i['cod_unif'], i['id'], 'false'

            print ("INICIANDO...")

            # VARS TO INSERT
            print ("INICIANDO VARIABLES...")

            #### Datos financieros
            _uni_ejec         = ""
            _anio_financ      = ""
            _pia              = 0.00
            _pim              = 0.00
            _dev              = 0.00
            _comp_anua        = 0.00
            _certi            = 0.00
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

            #||||||||||| INICIO ||||||||||||
            infoSiaf = requester.requestInfoSnip(codigoUnif,'SIAF',20)
            updateWithInfoSiafFinanciera(infoSiaf, codigoUnif, idProyecto)

    __init__()










