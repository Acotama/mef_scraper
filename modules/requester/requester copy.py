# -*- coding: utf-8 -*-
from decorate import retry
import json
import urllib.request as urllib2
import urllib
import requests
from bs4 import BeautifulSoup
import time
import socket
from urllib.error import HTTPError
import re

@retry(urllib2.URLError, tries=4, delay=3, backoff=2)
def requestToUrl(url):
    response = urllib2.urlopen(url).read().decode('utf-8')
    return response

## RequestInfoSNIP
def requestInfoSnip(codigo,tipo,tam):
    print ("~~ Consultando a SSI " + str(tipo) + " ~~~~~~~")
    url = "http://ofi5.mef.gob.pe/inviertews/Dashboard/traeDetInvSSI"
    auth_data = {'id': codigo,'tipo':tipo}

    session = requests.Session()
    session.headers = {
        "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
        "Content-Length": tam,
        "User-Agent":"Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36 OPR/77.0.4054.277",
        "Host": "ofi5.mef.gob.pe",
        "Origin": "https://ofi5.mef.gob.pe",
        "Accept-Encoding": "*",
        "Connection": "keep-alive"
    }
    # response = session.get(self.request_url)
    data = urllib.parse.urlencode(auth_data)
    data = data.encode('ascii') # data should be bytes
    req = urllib.request.Request(url, data, session.headers)
    response = urllib2.urlopen(req).read().decode('utf-8')
    if response:
        dataJSON = json.loads(response)
        return dataJSON
    else:
        return False

## RequestInfoSiaf
def requestInfoSiaf(codigo,flagSnip):
    print ("~~ Consultando a InfoSIAF ~~~~~~~")
    url = "http://ofi5.mef.gob.pe/inviertews/Dashboard/traeDetInvSSI"
    auth_data = {'id': codigo,'tipo':'SIAF'}
    try:
        response = requests.post(url, data=auth_data,verify=False)
        jsonstr = response.json()   
        response.close()   
        if len(jsonstr) != 0:
            return jsonstr
        else:
            return False
    except socket.gaierror as e:
        if e.errno == 10054:
            print("Error!")


def requestSSI(codigo,tipo,tam):
    print ("~~ Consultando a SSI " + str(tipo) + " ~~~~~~~")
    url = "https://ofi5.mef.gob.pe/inviertews/Dashboard/traeDevengSSI"
    auth_data = {'id': codigo,'tipo':tipo}
    session = requests.Session()
    session.headers = {
        "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
        "Content-Length": tam,
        "User-Agent":"Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36 OPR/77.0.4054.277",
        "Host": "ofi5.mef.gob.pe",
        "Origin": "https://ofi5.mef.gob.pe",
        "Accept-Encoding": "*",
        "Connection": "keep-alive"
    }
    data = urllib.parse.urlencode(auth_data)
    data = data.encode('ascii') # data should be bytes
    req = urllib.request.Request(url, data, session.headers)
    delay = 5
    max_retries = 3
    for _ in range(max_retries):
        try:
            response = urllib2.urlopen(req).read().decode('utf-8')
            if response:
                dataJSON = json.loads(response)
                return dataJSON
            else:
                return False
            break
        except urllib.error.URLError:
            time.sleep(delay)
            delay *= 2
    else:
        print(f"Fallo para {url} despues {max_retries} intentos")


def requestContrato(codigo):
    print ("~~ Consultando Contrataciones SSI ")
    url = "http://ofi5.mef.gob.pe/inviertews/DashboardSeace/traeContrSeaceSSI"
    auth_data = {'id': codigo,'vers':'v2'}
    session = requests.Session()
    session.headers = {
        "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
        "Content-Length": 18,
        "User-Agent":"Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36 OPR/77.0.4054.277",
        "Host": "ofi5.mef.gob.pe",
        "Origin": "https://ofi5.mef.gob.pe",
        "Accept-Encoding": "*",
        "Connection": "keep-alive"
    }
    data = urllib.parse.urlencode(auth_data)
    data = data.encode('ascii') # data should be bytes
    req = urllib.request.Request(url, data, session.headers)
    delay = 5
    max_retries = 3
    for _ in range(max_retries):
        try:
            response = urllib2.urlopen(req).read().decode('utf-8')
            if response:
                dataJSON = json.loads(response)
                return dataJSON
            else:
                return False
            break
        except urllib.error.URLError:
            time.sleep(delay)
            delay *= 2
    else:
        print(f"Fallo para {url} despues {max_retries} intentos")

## RequestInfoObras
def requestInfoObras(codigo,flagSnip):
    try:
        response = urllib2.urlopen(
            "http://ofi5.mef.gob.pe/ssi/wsSosem.asmx/ListarInfoObras?codigo=" + codigo).read()

        ini = response.find('['.encode())
        fin = response.find(']<'.encode())

        jsonstr = response[ini + 1:fin]

        infoObras = json.loads(jsonstr)
    except Exception:
        print (Exception)

## Request Mes Formato12-B
def requestMEF(codigo):
    print ("~~ Consultando a MEF FORMATO 12-B ~~~~~~~")
    url = "https://ofi5.mef.gob.pe/inviertews/Dashboard/traeInformF12B_CU"
    print ("Dirección => [" + url + "]")
    auth_data = {'id': codigo}
    response = requests.post(url, data=auth_data,verify=False)
    jsonstr = response.json()
      
    if len(jsonstr) != 0:
        return jsonstr
    else:
        return False

def requestverProcesoContratacionInv(codigo):
    print ("~~ Consultando Contrataciones ~~~~~~~")
    url = "https://ofi5.mef.gob.pe/inviertews/DashboardSeace/verProcesoContratacionInv/" + codigo
    print ("Dirección => [" + url + "]")

    response = requestToUrl(url)
    if response:
        jsonstr = response
        if len(jsonstr) != 0:
            contraciones = json.loads(jsonstr)
        else:
            contraciones = False
        return contraciones
    else:
        return False

def requestverCarteradeInversiones(codigo):
    if codigo is None:
        auth_data = {'ddlSector': '96','txtCodigoUnico': '','ddlGobiernoRegional': '464','ddlDepartamento': '','ddlMunProvincial': '','ddlMunDistrital': ''}
    else:
        auth_data = {'ddlSector': '','txtCodigoUnico': str(codigo)}
    print ("~~ Consultando Cartera de Inversiones ~~~~~~~")
    print ("Dirección => [https://ofi5.mef.gob.pe/invierte/pmi/consultapmi]")
    response = requests.post('https://ofi5.mef.gob.pe/invierte/Pmi/traeListaCarteraSector', data=auth_data,verify=False)
    
    jsonstr = response.json()
    if len(jsonstr) != 0:
        return jsonstr
    else:
        return False

def requestIOARREmergencia(codigo):
    print ("~~ Consultando Si Pertenece a ESTADO DE EMERGENCIA NACIONAL ~~~~~~~")
    url = "https://ofi5.mef.gob.pe/invierte/formato/verProyectoCU/" + codigo
    print ("Dirección => [" + url + "]")

    response = requestToUrl(url)
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
    print ("~~ Consultando F8 ~~~~~~~")
    url = "https://ofi5.mef.gob.pe/invierte/ejecucion/verFichaEjecucion/" + codigo
    print ("Dirección => [" + url + "]")
    try:
      response = requestToUrl(url)
    except HTTPError:
      return False

    if response:
        if len(response) != 0:
            expediente = response
        else:
            expediente = False
        return expediente
    else:
        return False


def data_et(codigo):
    mystring =  str(requestFormato08(codigo))
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

def data_et_c(codigo):
    mystring =  str(requestFormato08(codigo))
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
                array_et.append((row[aDataTableHeaderHTML-1]).strip())

        if (len(aDataTablePieHTML) > 0) :
            for row in aDataTablePieHTML :
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

        