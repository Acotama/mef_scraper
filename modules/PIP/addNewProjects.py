from conn import Conexion
from datetime import datetime
import reader
import requester
import pandas as pd
import json

def main(con):
    years = datetime.now().year 
    lista = requester.loadData()
    MaxID = 0

    def getCodSnip(codigo):        
        data = requester.requestInfoSnip(codigo,'SIAF',20)
        return data 

    def getList():
        con.getCur().execute("""(SELECT cod_unif FROM grli_pip_procompite) UNION (SELECT cod_unif FROM grli_pip_total_priori)""")
        result = con.getCur().fetchall()
        lista = [i['cod_unif'] for i in result]
        return lista

    def getListTotal():
        con.getCur().execute("""(select distinct cod_unif::int from  grli_pip_total_girado union select distinct codigo_unico from vw_cartera_pmi where codigo_unico is not null)""")
        result = con.getCur().fetchall()
        lista = [i['cod_unif'] for i in result]
        return lista

    def getMaxID():
        global MaxID
        con.getCur().execute("""SELECT MAX(id) as id FROM grli_pip_total_priori where id <> 1000""")
        # MaxID = con.getCur().fetchone()['id']
        resultado = con.getCur().fetchone()
        MaxID = resultado['id'] if resultado and resultado['id'] is not None else 0

    def insert(data,lista_pry):
    
        global MaxID
        f = '0'
        i = 0
        for row in data:     
            #Nombre Proyecto
            # nombreProyecto = row.contents[0].text
            #Codigo del proyecto
            codigo_snip = ""
            nombre = ""
            codigo = row
            flagFound = False
            if str(codigo) in lista_pry:
                flagFound = True
                continue

            if flagFound == False:
                getMaxID()
                MaxID += 1
                i += 1
                f = 1
                                
                #Nombre del proyecto
                data_inversion =requester.informacion_inversion(lista,int(codigo))
                if len(data_inversion) > 0:
                    data = data_inversion[0]
                    codigo_snip = data["COD_SNIP"] if len(str(data["COD_SNIP"])) > 0 else "SIN COD."
                    nombre = data["NOMBRE_INVERSION"]
                else:
                    data = getCodSnip(codigo)
                    if len(data) > 0:
                        data = data[0]
                        codigo_snip = data["COD_SNIP"] if len(str(data["COD_SNIP"])) > 0 else "SIN COD."
                        nombre = data["NOMBRE_INVERSION"]
                    else:
                        codigo_snip = "SIN COD." 
                   
                print(str(i) + ' => ' + str(codigo) + ': ' + str(nombre))
                
                if MaxID==1000:
                    MaxID += 1
                con.getCur().execute("""
                    INSERT INTO grli_pip_total_priori(id,cod_unif,cod_snip)
                    VALUES(%s, %s, %s) """,
                    (
                    str(MaxID),
                    str(codigo).strip(),
                    str(codigo_snip).strip(),
                    # str(years)
                )) 
                
                # ,anio

        if f == '0':
            print("NO HAY NUEVOS PROYECTOS PARA AGREGAR")
        else:
            print("GUARDANDO...")

    def insertData(data,lista_pry):
        global MaxID
        f = '0'
        for key,row in enumerate(data) :
            #Nombre Proyecto
            nombreProyecto = row.contents[0].text
            #Codigo del proyecto
            codigo = data[key]

            #Nombre del proyecto
            nombre = nombreProyecto.split(':')[1]
            flagFound = False
            
            if codigo in lista_pry:
                flagFound = True
                continue

            if flagFound == False:
                getMaxID()
                MaxID += 1
                f = 1
                # print(str(codigo) + ': ' + str(nombre))
                codigo_snip = getCodSnip(codigo,'false')
                if MaxID==1000:
                    MaxID += 1
                con.getCur().execute("""
                    INSERT INTO grli_pip_total_priori(id,cod_unif,cod_snip,anio)
                    VALUES(%s, %s, %s, %s) """,
                    (
                    str(MaxID),
                    str(codigo).strip(),
                    str(codigo_snip).strip(),
                    str(years)
                ))

        if f == '0':
            print("NO HAY NUEVOS PROYECTOS PARA AGREGAR")
        else:
            print("GUARDANDO...")


    def __init__():
        print("INICIANDO...")
        # document = './downloads/'+ str(years) +'/EjecucionPresupuestal/Proyectos/' + str(datetime.now().date()) + '.html'
        # data = reader.bs4Html(document)
        # trs = data.find('table', class_="Data").contents
        # trs= []

        insert(getListTotal(),getList())
        con.getConn().commit()


    # Iniciar
    __init__()
