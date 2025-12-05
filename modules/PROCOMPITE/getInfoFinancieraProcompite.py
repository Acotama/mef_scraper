import sys
import requester
from decimal import Decimal
from commons import twoDec
from importlib import reload
from datetime import datetime
import urllib.request as urllib2
import json
import decimal
from decimal import *
import time
import operator

reload(sys)

# FUNCTIONS
def main(con):
    now = datetime.now()
    def flagSiNo(str):
        if str == "1":
            return "Si"
        elif str == "0":
            return "No"
        else:
            return "---"


    # CONVERT TO 2 DECIMAL NUMBER
    def twoDec(num):
        num = Decimal(num).quantize(Decimal('1.00'))
        return num

    # UNIDADES EJECUTORAS
    unidadesEjecutoras = {
        1027: 'REGION LIMA',
        1325: 'SUB GERENCIA REGIONAL LIMA SUR',  # SGRLS
        1228: 'DIRECCIÓN REGIONAL DE AGRICULTURA LIMA PROVINCIAS',  # DRALP
        1190: 'DIRECCIÓN REGIONAL DE EDUCACION LIMA	PROVINCIAS',  # DRELP
        1181: 'UNIDAD DE GESTIÓN EDUCATIVA LOCAL CAÑETE',  # UGEL CAÑETE
        1182: 'UNIDAD DE GESTIÓN EDUCATIVA LOCAL HUAURA',  # UGEL HUAURA
        1183: 'UNIDAD DE GESTIÓN EDUCATIVA LOCAL HUARAL',  # UGEL HUARAL
        1184: 'UNIDAD DE GESTIÓN EDUCATIVA LOCAL CAJATAMBO',  # UGEL CAJATAMBO
        1185: 'UNIDAD DE GESTIÓN EDUCATIVA LOCAL CANTA',  # UGEL CANTA
        1186: 'UNIDAD DE GESTIÓN EDUCATIVA LOCAL YAUYOS',  # UGEL YAUYOS
        1187: 'UNIDAD DE GESTIÓN EDUCATIVA LOCAL OYON',  # UGEL OYON
        1188: 'UNIDAD DE GESTIÓN EDUCATIVA LOCAL HUAROCHIRI',  # UGEL HUAROCHIRI
        1189: 'UNIDAD DE GESTIÓN EDUCATIVA LOCAL BARRANCA',  # UGEL BARRANCA
        1285: 'DIRECCION DE SALUD III	LIMA NORTE',
        1286: 'HOSP.HUACHO - HUAURA - OYON Y SERV.BASICOS DE SALUD',
        1287: 'SERVICIOS BASICOS DE SALUD	CAÑETE - YAUYOS',
        1288: 'HOSPITAL DE APOYO REZOLA',
        1289: 'HOSP.BARRANCA - CAJATAMBO Y SERV.BASICOS DE SALUD',
        1290: 'HOSP.CHANCAY Y SERVICIOS BASICOS DE SALUD',
        1291: 'SERV.BASICOS DE SALUD CHILCA - MALA',
        1292: 'HOSPITAL HUARAL Y SERVICIOS BASICOS DE	SALUD',
        1404: 'GOB.REG.DE LIMA - RED DE	SALUD DE HUAROCHIRÍ'
    }
    
    def getProjects():
        print ("OBTENIENDO DATOS DE TB --> [grli_pip_procompite]")
        con.getCur().execute("""SELECT cod_unif, id FROM grli_pip_procompite order by id""")
        return con.getCur().fetchall()

    def __init__(): 
        print ("COMENZANDO PROCESO...")
        curi = getProjects()
        for i in curi:
            print ("********************************************************************************************* CODIGO SIAF <<" + i["cod_unif"] + ">> **********************************************************************************************")
            print (' ID -> ' + str(i["id"]))
            idPIP = int(i["id"])
            # VARS
            codigoUnif, flagSnip = i["cod_unif"], 'false'

            print("CONSULTANDO A SOSEM...")
            # VARS TO INSERT
            v = ''
            v = dict.fromkeys([
                '_nom_proyec',
                '_sector',
                '_m_pip',
                '_est_proyec',
                '_m_pim',
                '_m_cert'
                '_m_pimacu',
                '_m_deveng',
                '_m_devenacu',
                '_a_financ',
                '_fuente'], "")

            v['_m_pim'] = 0
            v['_m_cert'] = 0
            v['_m_pimacu'] = 0
            v['_m_deveng'] = 0
            v['_m_devenacu'] = 0

            ########################################################################################################################
            #################################################### INFO SIAF #########################################################
            ########################################################################################################################
            
            infoSiafdata = requester.requestInfoSnip(codigoUnif,'SIAF',20)

            for data in infoSiafdata:

                if data:

                    id = data['CodigoSiaf']

                    uELIST = unidadesEjecutoras.keys()

                    print ('Código SIAF -> ' + id)
                    print ('Nombre del Proyecto -> ' + str(data['Nombre']))
                    v['_nom_proyec'] = str(data['Nombre'])
                    print ('Tipo de Proyecto -> ' + data['TipoProyectoSnip'])

                    PimRegionalPasado = Decimal(0.00)
                    DevRegionalPasado = Decimal(0.00)
                    PimRegionalActual = Decimal(0.00)
                    DevRegionalActual = Decimal(0.00)
                    PIA = Decimal(0.00)
                    # ----------------------Fuente Financiera--------------------
                    print('---------------------------------INFORMACION FINANCIERA-------------------------------')
                    if len(data['InfoFinanciera']) > 0:
                        #print data
                        # OBTENGO AÑO MAS RECIENTE
                        lastYear = 0
                        for e in (data['InfoFinanciera']):
                            if int(e['CodigoEjecutora']) in uELIST and int(e["Año"]) > int(lastYear):
                                lastYear = e["Año"]
                        print("Ultimo año de movimiento financiero -> " + str(lastYear))

                        # ALFORITMO DE ORDENAMIENTO AÑO DE MENOR A MAYOR
                        for i in range(len(data['InfoFinanciera'])):
                            for j in range(len(data['InfoFinanciera']) - 1 - i):
                                if data['InfoFinanciera'][j]["Año"] > data['InfoFinanciera'][j + 1][
                                    "Año"]:
                                    data['InfoFinanciera'][j], data['InfoFinanciera'][j + 1] = data['InfoFinanciera'][j + 1], \
                                                                                            data['InfoFinanciera'][j]

                        ejecutoras = {}
                        for e in reversed(data['InfoFinanciera']):
                            if int(e['CodigoEjecutora']) in uELIST:
                                if e["Año"] == lastYear:
                                    PimRegionalActual = PimRegionalActual + twoDec(e['Pim'])
                                    DevRegionalActual = DevRegionalActual + twoDec(e['Dev'])
                                    #PIA = v['_mpia_pic']

                                    if int(e['CodigoEjecutora']) in ejecutoras.keys():
                                        ejecutoras[int(e['CodigoEjecutora'])] += twoDec(e['Pim'])
                                    else:
                                        ejecutoras[int(e['CodigoEjecutora'])] = twoDec(e['Pim'])
                                    v['_upDate'] = '31-12-' + str(e["Año"])
                                elif PimRegionalActual == 0.00 or PimRegionalActual == 0.0 or PimRegionalActual == 0 and e[
                                    "Año"] < lastYear:
                                    PimRegionalActual = PimRegionalActual + twoDec(e['Pim'])
                                    DevRegionalActual = DevRegionalActual + twoDec(e['Dev'])
                                    v['_upDate'] = '31-12-' + str(e["Año"])
                                    print((str(e["Año"])))

                                if int(lastYear) + 1 == now.year:
                                    v['_upDate'] = '31-12-' + str(lastYear)
                                if int(lastYear) == now.year:
                                    v['_upDate'] = time.strftime("%d-%m-%Y")

                                PimRegionalPasado = PimRegionalPasado + twoDec(e['Pim'])
                                DevRegionalPasado = DevRegionalPasado + twoDec(e['Dev'])

                        print('Por financiera ------------------')
                        print('Unidad ejecutora -> ' + unidadesEjecutoras[max(ejecutoras.items(), key=operator.itemgetter(1))[0]])
                        v['_u_ejec'] = unidadesEjecutoras[max(ejecutoras.items(), key=operator.itemgetter(1))[0]]
                        print('UNIDAD EJECUTORA =>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>' + str(v['_u_ejec']))
                        print('Año mas reciente de ejecucion -> ' + str(lastYear))
                        v['_m_pimacu'] = PimRegionalPasado
                        print('Pim Acumulado -> ' + str(v['_m_pimacu']))
                        v['_m_devenacu'] = DevRegionalPasado
                        print('Devengado Acumulado -> ' + str(v['_m_devenacu']))
                        v['_m_pim'] = PimRegionalActual
                        print('Pim Regional Actual -> ' + str(v['_m_pim']))
                        v['_m_deveng'] = DevRegionalActual
                        print('Devengado Regional Actual -> ' + str(v['_m_deveng']))

                        #print 'MONTO PIP -> ' + str(v['_m_devenacu'])

                        print(str(DevRegionalActual) + '!' + str(PimRegionalActual))

                        try:
                            print("AVANCE FINANCIERO ACUMULADO -> " + str(twoDec(Decimal(str(DevRegionalPasado)) / Decimal(str(PimRegionalPasado)) * 100)))
                            v['_a_financ'] = str(twoDec(Decimal(str(DevRegionalPasado)) / Decimal(str(PimRegionalPasado)) * 100))
                        except:
                            print('AVANCE FINANCIERO ACUMULADO ->' + '0.00')
                            v['_a_financ'] = '0.00'
                    else:
                        print('No hay InfoFinanciera .1')

                    # ----------------------Montos Siaf--------------------
                    if len(data['MontosSiaf']) > 0:
                        cantidadMontosSiaf = len(data['MontosSiaf'])
                        i = 0
                        for e in data['MontosSiaf']:
                            i = i + 1
                            # print(str(e))
                            if (i == cantidadMontosSiaf):
                                item = e

                                v['_m_cert'] = e['CertificadoAnual']
                                print('CERTIFICADO -> ' + str(v['_m_cert']))
                            else:
                                continue
                    else:
                        print('No hay InfoSiaf')
                else:
                    print('No hay InfoFinanciera')


            if str(v['_m_pimacu']) == '0' or str(v['_m_pimacu']) == '0.00' or str(v['_m_pimacu']) == '0.0':
                v['_upDate'] = ''


            ########################################################################################
            ####################################### ACTUALIZAR CAMPOS ##############################
            if v['_a_financ'] == '':
                v['_a_financ'] = '0.0'
            print(v['_a_financ'])

            print('Actualizado Al -> ' +str(v['_upDate']))

            updateRecords = "UPDATE grli_pip_procompite SET "
            updateRecords += "nom_proyec = (CASE WHEN '" + v['_nom_proyec'] + "' = '' THEN nom_proyec ELSE '" + v['_nom_proyec'] + "' END),"
            updateRecords += "m_pim      = (CASE WHEN '" + str(v['_m_pim']) + "' = '' THEN 0.00 WHEN '" + str(v['_m_pim']) + "' = '0.00' THEN m_pim WHEN '" + str(v['_m_pim']) + "' = '0' THEN m_pim ELSE '" + str(v['_m_pim']) + "' END),"
            updateRecords += "m_pimacu  = (CASE WHEN '" + str(v['_m_pimacu']) + "' = '' THEN 0.00 WHEN '" + str(v['_m_pimacu']) + "' = '0.00' THEN m_pimacu WHEN '" + str(v['_m_pimacu']) + "' = '0' THEN m_pimacu ELSE '" + str(v['_m_pimacu']) + "' END),"
            updateRecords += "m_cert = (CASE WHEN '" + str(v['_m_pimacu']) + "' = '' THEN 0.00 WHEN '" + str(v['_m_pimacu']) + "' = '0.00' THEN m_cert WHEN '" + str(v['_m_pimacu']) + "' = '0' THEN m_cert ELSE '" + str(v['_m_pimacu']) + "' END),"
            updateRecords += "f_act = (CASE WHEN '" + v['_upDate'] + "' = f_act THEN f_act WHEN '" + v['_upDate'] + "' = '' THEN f_act ELSE '" + v['_upDate'] + "' END),"
            # Si existe pim entonces devengado puede ser 0
            if v['_m_pim'] > 0:
                updateRecords += "m_deveng   = (CASE WHEN '" + str(v['_m_deveng']) + "' = '' THEN 0.00 ELSE '" + str(           v['_m_deveng']) + "' END),"
            else:
                updateRecords += "m_deveng   = (CASE WHEN '" + str(v['_m_deveng']) + "' = '' THEN 0.00 WHEN '" + str(v['_m_deveng']) + "' = '0.00' THEN m_deveng WHEN '" + str(v['_m_deveng']) + "' = '0' THEN m_deveng ELSE '" + str(v['_m_deveng']) + "' END),"
            
            updateRecords += "m_devenacu = (CASE WHEN '" + str(v['_m_devenacu']) + "' = '' THEN 0.00 WHEN '" + str(v['_m_devenacu']) + "' = '0.0' THEN m_devenacu WHEN '" + str(v['_m_devenacu']) + "' = '0.00' THEN m_devenacu WHEN '" + str(v['_m_devenacu']) + "' = '0' THEN m_devenacu ELSE '" + str(v['_m_devenacu']) + "' END),"
            updateRecords += "a_financ   = (CASE WHEN '" + v['_a_financ'] + "' = '0' THEN a_financ WHEN '" + v['_a_financ'] + "' = '0.00' THEN '0.00' ELSE '" + v['_a_financ'] + "' END)"
            updateRecords += "WHERE "
            updateRecords += "id   = " + str(idPIP) + ";"

            # print updateRecords
            con.getCur().execute(updateRecords)
            # conn.commit()
        
    __init__()
    