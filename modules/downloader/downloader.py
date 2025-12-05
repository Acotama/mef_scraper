import sys
import download
import datetime
import requests
import urllib.request as urllib2
from bs4 import BeautifulSoup
import json
import os
import reader
import errno
from decimal import Decimal

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

def EjecucionPresupuestalProyectosRank(anio):
    url = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400&of=col7&od=1"
    docName = str(datetime.datetime.now().date()) + '.html'
    destiny = './downloads/' + str(anio) + '/Rank/Proyectos/' + docName
    download.downloadFromConsultaAmigable(url,headers,destiny)

def EjecucionPresupuestalActividadesRank(anio):
    url = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=&y="+str(anio)+"&cpage=1&psize=400&of=col7&od=1"
    docName = str(datetime.datetime.now().date()) + '.html'
    destiny = './downloads/' + str(anio) + '/Rank/Actividades/' + docName
    download.downloadFromConsultaAmigable(url,headers,destiny)

def EjecucionPresupuestalProyectos(anio):
    url = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&31=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=3000"
    docName = str(datetime.datetime.now().date()) + '.html'
    destiny = './downloads/' + str(anio) + '/EjecucionPresupuestal/Proyectos/' + docName
    download.downloadFromConsultaAmigable(url,headers,destiny)

def EjecucionPresupuestalProyectosRecursosOrdinarios(anio):
    url ="https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&14=1&31=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
    docName = str(datetime.datetime.now().date()) + '.html'
    destiny = './downloads/' + str(anio) + '/EjecucionPresupuestal/RECURSOS_ORDINARIOS/' + docName
    download.downloadFromConsultaAmigable(url,headers,destiny)

def EjecucionPresupuestalProyectosRecursosDeterminados(anio):
    url ="https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&14=5&31=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
    docName = str(datetime.datetime.now().date()) + '.html'
    destiny = './downloads/' + str(anio) + '/EjecucionPresupuestal/RECURSOS_DETERMINADOS/' + docName
    download.downloadFromConsultaAmigable(url,headers,destiny)

def EjecucionPresupuestalProyectosRecursosPorOperacionesOficialesDeCredito(anio):
    url ="https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&14=3&31=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
    docName = str(datetime.datetime.now().date()) + '.html'
    destiny = './downloads/' + str(anio) + '/EjecucionPresupuestal/RECURSOS_POR_OPERACIONES_OFICIALES_DE_CREDITO/' + docName
    download.downloadFromConsultaAmigable(url,headers,destiny)

def EjecucionPresupuestalProyectosDonacionesyTransferencias(anio):
    url ="https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&14=4&31=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
    docName = str(datetime.datetime.now().date()) + '.html'
    destiny = './downloads/' + str(anio) + '/EjecucionPresupuestal/DONACIONES_TRANSFERENCIAS/' + docName

    download.downloadFromConsultaAmigable(url,headers,destiny)

def EjecucionPresupuestalProyectosRecursosDirectamenteRecaudados (anio):
    url ="https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&14=2&31=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
    docName = str(datetime.datetime.now().date()) + '.html'
    destiny = './downloads/' + str(anio) + '/EjecucionPresupuestal/RECURSOS_DIRECTAMENTE_RECAUDADOS/' + docName

    download.downloadFromConsultaAmigable(url,headers,destiny)

def EjecucionPresupuestalMantRutinarioVias(anio):
    url = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&30=0138&31=3000132&32=5001449&8=15&33=033&34=0065&13=&y="+str(anio)+"&ap=Actividad&cpage=1&psize=3000"
    docName = str(datetime.datetime.now().date()) + '.html'
    destiny = './downloads/' + str(anio) + '/EjecucionPresupuestal/Vias/Rutinario/' + docName
    download.downloadFromConsultaAmigable(url,headers,destiny)

def EjecucionPresupuestalMantPeriodicoVias(anio):
    url = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&30=0138&31=3000132&32=5002376&8=15&33=033&34=0065&13=&y="+str(anio)+"&ap=Actividad&cpage=1&psize=3000"
    docName = str(datetime.datetime.now().date()) + '.html'
    destiny = './downloads/' + str(anio) + '/EjecucionPresupuestal/Vias/Periodico/' + docName
    download.downloadFromConsultaAmigable(url,headers,destiny)

def EjecucionPresupuestalProyectosRankSectorRegional(anio):
    url = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&8=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
    docName = str(datetime.datetime.now().date()) + '.html'
    destiny = './downloads/' + str(anio) + '/Rank/Proyectos/Sector/Regional/' + docName
    download.downloadFromConsultaAmigable(url,headers,destiny)

def EjecucionPresupuestalProyectosRankSectorRegionLima(anio):
    url = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&8=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"       
    docName = str(datetime.datetime.now().date()) + '.html'
    destiny = './downloads/' + str(anio) + '/Rank/Proyectos/Sector/RegionLima/' + docName
    download.downloadFromConsultaAmigable(url,headers,destiny)

def ListaInvOPMIJson(anio):
    url = "https://ofi5.mef.gob.pe/inviertews/Dashboard/verListaInvOPMI?id=21158&tipo=INF"
    # https://ofi5.mef.gob.pe/inviertews/Repseguim/RepOpmi?opmi=21158
    print ("Dirección => [" + url + "]")
    docName = 'DataInv.json'
    destiny = './downloads/' + str(anio) + '/EjecucionPresupuestal/ProyectosJSON/' 

    try:    
        response = urllib2.urlopen(url).read()
        dataJSON = json.loads(response)
        
        # dataOBJ = {}
        # for data in dataJSON:
        #     dataOBJ[data['CODIGO_UNICO']] = []
        #     dataOBJ[data['CODIGO_UNICO']].append(data)
        with open(os.path.join(destiny, docName), 'w') as file:
            json.dump(dataJSON, file)
    except:
        print("Ocurrio un error en la Descarga")

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

    url_mes = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&23=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
    docName_mes = str(datetime.datetime.now().date()) + '.html'
    destiny_mes = './downloads/' + str(anio) + '/EjecucionPresupuestal/ProyectoMes/' + docName_mes
    try:    
        download.downloadFromConsultaAmigable(url_mes,headers,destiny_mes)
    except:
        print("Ocurrio un error en la Descarga")

    data = reader.bs4Html("./downloads/"+ str(anio) +"/EjecucionPresupuestal/ProyectoMes/" + str(docName_mes))
    trs = data.find("table", class_="Data").contents
    meses = set([])
    for row in trs:
        mes = row.contents[0].text.split(':')[0].strip()
        print("Mes :" + mes)
        dev_dia = row.contents[6].text.replace(",", "")
        dev_dia = dev_dia.replace(".", ",")
        dev_dia = Decimal(dev_dia) if len(dev_dia) != 0 else Decimal(0)
        devengado_db = monto_mes(con,(mes))
        if dev_dia > devengado_db + 1:
            meses.add(mes)
        
    mes_c = datetime.datetime.now().month
    if mes_c != 1:
        if int(datetime.datetime.now().day) == 1 :
            mes_c = mes_c - 1
        else:
            mes_c = mes_c  
    else:
        mes_c=mes_c

    if anio <= 2022:
        mes_c = 12

    meses.add(mes_c)
    return meses

def EjecucionPresupuestalProyectosMes(con,anio):

    meses = monto_cambies_mef(con,anio)
    
    for mes in meses:
        print(" ============================ MES " + str(mes) + " ============================ ")
        url = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&23="+str(mes)+"&31=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
        docName = str(datetime.datetime.now().date()) + '.html'
        destiny = './downloads/' + str(anio) + '/EjecucionPresupuestal/ProyectoMes/' + str(mes) +'/' + docName

        try:    
            download.downloadFromConsultaAmigable(url,headers,destiny)
        except:
            print("Ocurrio un error en la Descarga")


def Lista_Consulta_Grupo(anio,carpeta):

    url = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&8=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
           
    docName = str(datetime.datetime.now().date()) + '.html'
    carpeta_principal = carpeta
    destiny = './downloads/' + str(anio) + '/ejecucionPresupuestal/'+str(carpeta_principal)+'/' + docName
    download.downloadFromConsultaAmigable(url,headers,destiny)

    categoria = "/EjecucionPresupuestal/"+str(carpeta_principal)+"/"
    ruta= "downloads/" + str(anio) + categoria

    data = reader.bs4Html("downloads/"+ str(anio) +"/EjecucionPresupuestal/"+str(carpeta_principal)+"/" + docName)
    trs = data.find("table", class_="Data").contents
    for row in trs:
        col = row.contents[0].text
        codigo = col.split(':')[0].strip()
        nombredenominacion = col.split(':')[1].strip()
        nombre_carpeta = nombredenominacion
        # Crea las carpetas
        try:
            os.mkdir(ruta + nombre_carpeta)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
#         #Guarda los datos
        if carpeta == 'Funcion':
            print('FUNCION - ' + str(nombre_carpeta) + ':')
            url_v = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3=463&8="+str(codigo)+"&31=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
        elif carpeta == 'Fuente':
            url_v = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3="+str(gr)+"&14="+str(codigo)+"&31=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
        elif carpeta == 'Rubro':
            url_v = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3="+str(gr)+"&15="+str(codigo)+"&31=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
        elif carpeta == 'ProyectoMes':
            url_v = "https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_tgt=xls&_uhc=yes&0=&1=R&2=99&3="+str(gr)+"&23="+str(codigo)+"&31=&y="+str(anio)+"&ap=Proyecto&cpage=1&psize=400"
        else:
            print("Verificar Link")
            return
        
        docName_v = str(datetime.datetime.now().date()) + '.html'
        destiny_v = './downloads/' + str(anio) + '/ejecucionPresupuestal/'+str(carpeta_principal)+'/' + nombre_carpeta + "/" + docName_v
        download.downloadFromConsultaAmigable(url_v,headers,destiny_v)