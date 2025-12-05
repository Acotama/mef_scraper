# Decorator
import sys
sys.path.insert(0, './modules/decorator')
from decorate import retry
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import urllib.parse
import urllib.request
import certifi
import ssl

import json
import re
import requests
import pandas as pd
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from urllib.error import HTTPError


# ssl._create_default_https_context = ssl._create_unverified_context
# import ssl
context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
# urllib.request.urlopen(req,context=context)
     
@retry(urllib.error.URLError, tries=4, delay=3, backoff=2)
def requestToUrl(url,values):
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
    # ua = UserAgent()
    # print(ua.random)
    headers = {'User-Agent': user_agent}
    if len(values) > 0 :
        data = urllib.parse.urlencode(values)
        data = data.encode('ascii')
        req = urllib.request.Request(url, data, headers)
    else :
        req = urllib.request.Request(url)
        
    with urllib.request.urlopen(req, context=context) as response:
        print('Estado de Consulta : ' + str(response.getcode()))
        return response.read().decode('utf-8')

@retry(urllib.error.URLError, tries=4, delay=3, backoff=2)
def requestToUrlCode(url,values):
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
    headers = {'User-Agent': user_agent}
    if len(values) > 0 :
        data = urllib.parse.urlencode(values)
        data = data.encode('ascii')
        req = urllib.request.Request(url, data, headers)
    else :
        req = urllib.request.Request(url)
        
    with urllib.request.urlopen(req, context=context) as response:
        print('Estado de Consulta : ' + str(response.getcode()))
        return response.getcode()

def requestInfoSnip(codigo,tipo,tam):
    print ("~~ Consultando a SSI " + str(tipo) + " ~~~~~~~")
    url = "http://ofi5.mef.gob.pe/inviertews/Dashboard/traeDetInvSSI"
    values = {'id': codigo,'tipo':tipo}
    
    response = requestToUrl(url,values)
    if response:
        dataJSON = json.loads(response)
        return dataJSON
    else:
        return False

def requestInfoSiaf(codigo,flagSnip):
    print ("~~ Consultando a InfoSIAF ~~")
    url = "http://ofi5.mef.gob.pe/inviertews/Dashboard/traeDetInvSSI"
    values = {'id': codigo,'tipo':'SIAF'}
    
    response = requestToUrl(url,values)
    if response:
        dataJSON = json.loads(response)
        return dataJSON
    else:
        return False

def requestSSI(codigo,tipo,tam):
    print ("~~ Consultando a SSI ~~")
    url = "https://ofi5.mef.gob.pe/inviertews/Dashboard/traeDevengSSI"
    values = {'id': codigo,'tipo':tipo}
    
    response = requestToUrl(url,values)
    if response:
        dataJSON = json.loads(response)
        return dataJSON
    else:
        return False

def requestContrato(codigo):
    print ("~~ Consultando Contrataciones SSI ")
    url = "http://ofi5.mef.gob.pe/inviertews/DashboardSeace/traeContrSeaceSSI"
    values = {'id': codigo,'vers':'v2'}
    
    response = requestToUrl(url,values)
    if response:
        dataJSON = json.loads(response)
        return dataJSON
    else:
        return False

def requestMEF(codigo):
    print ("~~ Consultando a MEF FORMATO 12-B ~~~~~~~")
    url = "https://ofi5.mef.gob.pe/inviertews/Dashboard/traeInformF12B_CU"
    values = {'id': codigo}
    
    response = requestToUrl(url,values)
    if response:
        dataJSON = json.loads(response)
        return dataJSON
    else:
        return False

def requestverProcesoContratacionInv(codigo,tipo):
    print ("~~ CONSULTA DE CONTRATACIONES ~~")
    if(tipo == 1 ):
        url = "https://ofi5.mef.gob.pe/inviertews/DashboardSeace/verProcesoContratacionInv/" + codigo
    elif(tipo == 2 ):
        url = "https://ofi5.mef.gob.pe/inviertews/DashboardSeace/verContratacionSeace/" + codigo
    elif(tipo == 3 ):
        url = "https://ofi5.mef.gob.pe/inviertews/DashboardSeace/verContratoAsociado?id=" + codigo + "&vers=v2"

    values = {}
    response = requestToUrl(url,values)
    if response:
        dataJSON = json.loads(response)
        return dataJSON
    else:
        return False

def requestverCarteradeInversiones(codigo):
    print ("~~ Consultando Cartera de Inversiones ~~~~~~~")
    url = 'https://ofi5.mef.gob.pe/invierte/Pmi/traeListaCarteraSector'
    if codigo is None:
        values = {'ddlSector': '96','txtCodigoUnico': '','ddlGobiernoRegional': '464','ddlDepartamento': '','ddlMunProvincial': '','ddlMunDistrital': ''}
    else:
        values = {'ddlSector': '','txtCodigoUnico': str(codigo)}

    response = requestToUrl(url,values)
    print(response)
    if response:
        dataJSON = json.loads(response)
        return dataJSON
    else:
        return False

def requestIOARREmergencia(codigo):
    print ("~~ Consultando Si Pertenece a ESTADO DE EMERGENCIA NACIONAL ~~~")
    url = "https://ofi5.mef.gob.pe/invierte/formato/verProyectoCU/" + codigo
    values = {}
    response = requestToUrl(url,values)
    if response:
        bsObj = BeautifulSoup(response,"html.parser")
        text = str(bsObj.h3).strip()
        if text.find("FORMATO N° 07-D") >0 or text.find("FORMATO N° 07-C") >0 : 
            ini = text.find('>')
            fin = text.find('</')
            data = text[ini+1:fin]
            return data.strip()
        return None
    else:
        return False

def requestFormato08(codigo):
    print ("~~ Consultando F8 ~~")
    url = "https://ofi5.mef.gob.pe/invierte/ejecucion/verFichaEjecucion/" + codigo
    try:
        values = {}
        response = requestToUrl(url,values)
        # print(response.getcode())
        if response:
            if len(response) != 0:
                expediente = response
            else:
                expediente = False
            return expediente
        else:
            return False
    except HTTPError as e:
        content = e.read()
    # return
    # if r.status_code != 500:
    #     response = requestToUrl(url,values)
    #     # print(response.getcode())
    #     if response:
    #         if len(response) != 0:
    #             expediente = response
    #         else:
    #             expediente = False
    #         return expediente
    #     else:
    #         return False
    # else:
    #     return False

def brechas(codigo):
    mystring =  str(requestFormato08(codigo))
    cantidad = len(mystring)
    if cantidad > 0 :
        findme   = str("Articulación con el programa multianual de inversiones (PMI")
        inicio = mystring.find(findme)
        tabla = None
        data_brecha = []
        if (inicio != -1) :
            cortado = mystring[inicio:cantidad]
            # cortado_cantidad = len(cortado)
            inicio_tabla = cortado.find("<table") 
            fin_tabla = cortado.find("</table")
            tabla = cortado[inicio_tabla:fin_tabla - inicio_tabla + 10]
            table = BeautifulSoup(tabla, 'html.parser')
            tbody = table.find_all('tbody')
            if len(tbody) > 0:
                td = tbody[0].find_all('td')
                data_brecha = {'BRECHA': td[0].string.strip(),'INDICADOR' : td[1].string.strip(),'UNIDAD_MEDIDA' : td[2].string.strip(),'ESPACIO_GEO' : td[3].string.strip(),'CONT_CIERRE' : td[4].string.strip()}
            else:
                data_brecha = []
        return data_brecha
    else:
        return None

def data_et(codigo,data):
    mystring =  str(data)
    cantidad = len(mystring)
    findme   = str("C. Datos de la fase de Ejecución; durante la ejecución física")
    inicio = mystring.find(findme)
    tabla = None
    tabla_b = None
    text = ""
    findme_b  = ["B. Datos en la fase de Ejecución","B. Datos de la fase de Ejecución"]
    if(mystring.find(findme_b[0])  != -1) :
        inicio_b = mystring.find(findme_b[0])
    elif (mystring.find(findme_b[1])  != -1) :
        inicio_b = mystring.find(findme_b[1]) 
    else:
        inicio_b = -1
    if (inicio != -1) :
        cortado = mystring[inicio:cantidad]
        cortado_cantidad = len(cortado)
        inicio_tabla = cortado.find("<table") 
        fin_tabla = cortado.find("</table")
        tabla = cortado[inicio_tabla:fin_tabla - inicio_tabla + 10]
        if (inicio_b != -1) :
            cortado = mystring[inicio_b:cantidad]
            cortado_cantidad = len(cortado)
            inicio_tabla = cortado.find("<table")
            fin_tabla = cortado.find("</table")
            tabla_b = cortado[inicio_tabla:fin_tabla - inicio_tabla + 10]
        text= "Cuenta con ET"
    else :
        if (inicio_b != -1) :
            cortado = mystring[inicio_b:cantidad]
            cortado_cantidad = len(cortado)
            inicio_tabla = cortado.find("<table")
            fin_tabla = cortado.find("</table")
            tabla_b = cortado[inicio_tabla:fin_tabla - inicio_tabla + 10]
        else:
            text= "No cuenta con ET"
    
    et=[]
    et = expediente(tabla,12,8)
    if(et[0]['et'] == 'SIN ET') :
        et = expediente(tabla_b,9,3)   
    return et

def data_et_c(codigo,data):
    mystring =  str(data)
    cantidad = len(mystring)
    findme   = str("C. Datos de la fase de Ejecución; durante la ejecución física")
    if(mystring.find(findme)  != -1) :
        return "SI"
    else:      
        return "NO"

def is_empty(a):
    return len(a) == 0

def expediente(tabla,colHeader,colFooter) :
    if tabla != None:
        table = BeautifulSoup(tabla, 'html.parser')
        tr = table.find_all('tr')

        Detail = []
        Footer = []
        aDataTableHeaderHTML = colHeader
        aDataTableFooterHTML = colFooter
        for key, Nodetr in enumerate(tr):
            if(len(Nodetr.find_all('td')) == aDataTableHeaderHTML):
                Detail.append(Nodetr.find_all('td'))
            elif(len(Nodetr.find_all('td')) == aDataTableFooterHTML):
                Footer.append(Nodetr.find_all('td'))

        i = 0
        j = 0
        aDataTableDetailHTML = []
        for sNodeDetail in Detail :
            data = []
            for item in sNodeDetail:  
                er = item.find_all('a')
                if len(er) > 0 and len(er[0]["href"]) > 0 :
                    ed = er[0]["href"]
                    data.append(("https://ofi5.mef.gob.pe" + ed).strip()  + "||"  + (item.text).strip())
                else :
                    data.append((item.text).strip())
            aDataTableDetailHTML.append(data)
                    
        i = 0
        j = 0
        aDataTablePieHTML = []
        for sNodeFooter in Footer :
            data = []
            for item in sNodeFooter:  
                er = item.find_all('a')
                if len(er) > 0 and len(er[0]["href"]) > 0 :
                    ed = er[0]["href"]
                    data.append(("https://ofi5.mef.gob.pe" + ed).strip()  + "||"  + (item.text).strip())
                else :
                    data.append((item.text).strip())
            aDataTablePieHTML.append(data)
              
        # Obtemos Nombre y Enlace
        array_et = []
        if (len(aDataTableDetailHTML) > 0) :
            for row in aDataTableDetailHTML :
                if len(row[aDataTableHeaderHTML-1].split('||')) == 2:
                    if len((row[aDataTableHeaderHTML-1].split('||')[1]).strip()) > 0:
                        array_et.append((row[aDataTableHeaderHTML-1]).strip())

        if (len(aDataTablePieHTML) > 0) :
            for row in aDataTablePieHTML :
                if len(row[aDataTableFooterHTML-1].split('||')) == 2:
                    if len((row[aDataTableFooterHTML-1].split('||')[1]).strip()) > 0:
                        array_et.append((row[aDataTableFooterHTML-1]).strip())
  
        array_et_conjunto = []
        array_et_no_vacias = [x for x in array_et if x != '']

        resultado = sorted(set(array_et_no_vacias))
        et_final = []
        if (len(resultado) > 0) :
            for key, row in enumerate(resultado) :
                url = row.split('||')[0]
                nombre_et = (row.split('||')[1]).strip()
                fecha = (((nombre_et.replace('/','-')).split('(')[1]).replace(')','')).strip()
                nombre = ((((nombre_et.replace('/','-')).split('(')[0]).replace(')','')).strip())
                array_et_conjunto.append({'et' : nombre,'fecha'  : fecha,'id'  : key,'url'  : url})
            resmatch = r"(?:(?:(\AR.{1,})[\s\S](N(°|º))[\s\S]|(\AN(°|º))[\s\S])(\d{1,})(.)(\d{1,})|(?:(\AR.{1,})|(\A\d{1,})(.)(\d{1,})))"
            count_et = []
            for key,row in enumerate(array_et_conjunto) :
                string = row['et']
                res = re.search(resmatch, string, re.IGNORECASE)
                if res is not None:
                    count_et.append(key)
            if(len(count_et) > 1) :
                array = [] 
                for key,row in enumerate(count_et) :
                    array.append(array_et_conjunto[row])
                
                array = sorted(array, key=lambda  fecha: fecha['fecha'])
                et_final.append({'et' :  array[0]['et'],'fecha'  :  array[0]['fecha'],'url'  :  array[0]['url']})
            elif(len(count_et) == 1):
                et_final.append({'et' :  array_et_conjunto[count_et[0]]['et'],'fecha' : array_et_conjunto[count_et[0]]['fecha'],'url'  :  array_et_conjunto[count_et[0]]['url']})
            else:
                et_final.append({'et': "SIN ET"})
        else:
            et_final.append({'et': "SIN ET"})
        return et_final  
    else:
        et=[]
        et.append({'et': "SIN ET"})
        return et

def etapa_f8(data):
    if data is not None:
        soup = BeautifulSoup(data,"html.parser")
        etapa = soup.find("strong")
        print(etapa)
        return (etapa.text).strip()
    else:
        return None

def et_def12b(codigo):
    print ("~~ Consultando Expediente Tecnico o ED ~~~~~~~")
    url = "https://ofi5.mef.gob.pe/inviertews/Dashboard/verListaET/" + codigo
    values = {}
    
    response = requestToUrl(url,values)
    if response:
        dataJSON = json.loads(response)
        return dataJSON
    else:
        return False

def detalle_et_def12b(codigo,tipo):
    print ("~~ Consultando detalle ET o DE ~~~~~~~")
    _tipo = ''
    if tipo == 'ELABORACIÓN DEL ET - APROBACIÓN DEL ET' :
        _tipo = 'NVO'
    elif tipo == 'ELABORACIÓN DE ET - APROBACIÓN':
        _tipo = 'ANT'
    else:
        _tipo = ''

    url = "https://ofi5.mef.gob.pe/inviertews/Dashboard/verDetalleET?id=" + codigo + "&tipo=" + _tipo
    values = { }
    
    response = requestToUrl(url,values)
    if response:
        dataJSON = json.loads(response)
        return dataJSON
    else:
        return False

def consulta_ex_de(codigo_unico):
    print("========================> " + str(codigo_unico) + " <========================")
    data = et_def12b(str(codigo_unico))
    if len(data) > 0:
        fecha = []
        fecha_max = ''
        data_fecha = []
        data_detalle = []
        parametros = []
        for item in data:
            fecha.append(datetime.strptime(item['FEC_REG_ET'], '%d/%m/%Y'))

        fecha_max = max(fecha)
        data_fecha = [x for x in data if datetime.strptime(x['FEC_REG_ET'], '%d/%m/%Y') == fecha_max ]
        parametros = [{'ID_EXP_TECNICO':x['ID_EXP_TECNICO'],'DES_ETAPA':x['DES_ETAPA']} for x in data_fecha]
        
        data_detalle = detalle_et_def12b(str(parametros[0]['ID_EXP_TECNICO']),str(parametros[0]['DES_ETAPA']))
        data_resultado = []
        data = []
        # print(parametros)
        if len(data_detalle) > 0:
            if parametros[0]['DES_ETAPA'] == 'ELABORACIÓN DEL ET - APROBACIÓN DEL ET':
                FEC_PROGRAM = None
                FEC_ACTUALIZADA = None
                ESTADO_HITO = None
                data = data_detalle
                for item in data_detalle:
                    if len(item['FEC_PROGRAM'].strip()) > 0 :
                        FEC_PROGRAM = 'SI'
                    else:
                        FEC_PROGRAM = 'NO'
                        
                    if len(item['FEC_ACTUALIZADA'].strip()) > 0:
                        FEC_ACTUALIZADA = 'SI'
                    else:
                        FEC_ACTUALIZADA = 'NO'
                        
                    if len(item['ESTADO_HITO'].strip()) > 0:
                        ESTADO_HITO = 'SI'
                    else:
                        ESTADO_HITO = 'NO'
                        
                    if FEC_PROGRAM == None or FEC_ACTUALIZADA == None or ESTADO_HITO == None:
                        break
                        
                data_resultado.append({'codigo_unico':str(codigo_unico),'fec_program':FEC_PROGRAM,'fec_actualizada':FEC_ACTUALIZADA,'estado_hito':ESTADO_HITO,'fec_final':'NO','tipo':'NVO'})
                data.insert(0,{'tipo':'NVO'})
                return data_resultado,data
            elif parametros[0]['DES_ETAPA'] == 'ELABORACIÓN DE ET - APROBACIÓN':
                data_detalle = [x for x in data_detalle if len(x['COMENTARIO'].strip()) == 0 ]
                data_resultado = []
                FEC_PROGRAM = None
                FEC_ACTUALIZADA = None
                FEC_FINAL = None
                data = data_detalle
                for item in data_detalle:
                    if len(item['FEC_PROGRAM'].strip()) > 0 :
                        FEC_PROGRAM = 'SI'
                    else:
                        FEC_PROGRAM = 'NO'
                        
                    if len(item['FEC_ACTUALIZADA'].strip()) > 0:
                        FEC_ACTUALIZADA ='SI'
                    else:
                        FEC_ACTUALIZADA = 'NO'
                        
                    if len(item['FEC_FINAL'].strip()) > 0:
                        FEC_FINAL = 'SI'
                    else:
                        FEC_FINAL = 'NO'
                        
                    if FEC_PROGRAM == 'NO' or FEC_ACTUALIZADA == 'NO' or FEC_FINAL == 'NO':
                        break
                        
                data_resultado.append({'codigo_unico':str(codigo_unico),'fec_program':FEC_PROGRAM,'fec_actualizada':FEC_ACTUALIZADA,'fec_final':FEC_FINAL,'estado_hito':'NO','tipo':'ANT'})
                data.insert(0,{'tipo':'ANT'})
                return data_resultado,data
            else:
                return [],[]
        else:
            return [],[]
    else:
        return [],[]

def loadData():
    # with open('./downloads/2023/EjecucionPresupuestal/ProyectosJSON/DataInv.json') as f:
    #     lista = json.loads(f.read())
    #     return lista
    return pd.read_json('./downloads/2023/EjecucionPresupuestal/ProyectosJSON/DataInv.json')

def informacion_inversion(lista,cod_unif):

    data = (lista[lista["CODIGO_UNICO"] == cod_unif])
    data = data.drop_duplicates(['CODIGO_UNICO'], keep='last')
    data_json = data.to_json(orient = 'records')
    # print(data_json['SECTOR'])
    # for row in data_json:
    data_json = json.loads(data_json)
    return data_json
    # for row in data_json :
    #     print(row['CODIGO_UNICO'])