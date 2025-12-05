# -*- coding: utf-8 -*-
import sys

# Decorator
sys.path.insert(0, './modules/decorator')
# Conn Module
sys.path.insert(0, './conn')
sys.path.insert(0, './modules/downloader')
sys.path.insert(0, './modules/requester')
sys.path.insert(0, './modules/tools')
sys.path.insert(0, './modules/reader')
sys.path.insert(0, './modules/PIP')
sys.path.insert(0, './modules/RANK')
sys.path.insert(0, './modules/SEGUIMIENTO')
sys.path.insert(0, './modules/PROCOMPITE')
sys.path.insert(0, './modules/VIAS')
sys.path.insert(0, './modules/CARTERA_PMI')

import downloader
import updateTblGirado
import addNewProjects
import updateFuente
import updateTblDiario
import updateTblSeguimiento
import updateTblSeguimientoGrupo
import updateTblPip
import updateTblVias
import getInfoFinancieraHistoricaProcompiteSOSEM
import getInfoFinancieraProcompite
import updateTblRankProjects
import updateSectores
import verificar_et
import updateFormato12B
import addcarterapmi
from datetime import datetime
from conn import Conexion
#def __init__():
    #conn.connect()

con = Conexion()
con.connect()

def dailyDownloads(years):
    print("=====================AÑO " + str(years) + "=====================")
    downloader.EjecucionPresupuestalProyectos(years)
    downloader.EjecucionPresupuestalProyectosRecursosOrdinarios(years)
    downloader.EjecucionPresupuestalProyectosRecursosDeterminados(years)
    downloader.EjecucionPresupuestalProyectosRecursosPorOperacionesOficialesDeCredito(years)
    downloader.EjecucionPresupuestalProyectosDonacionesyTransferencias(years)
    downloader.EjecucionPresupuestalProyectosRecursosDirectamenteRecaudados(years)
    downloader.EjecucionPresupuestalMantRutinarioVias(years)
    downloader.EjecucionPresupuestalMantPeriodicoVias(years)
    downloader.EjecucionPresupuestalProyectosRank(years)
    downloader.EjecucionPresupuestalActividadesRank(years)
    downloader.EjecucionPresupuestalProyectosRankSectorRegional(years)
    # downloader.ListaInvOPMIJson(years)
    downloader.EjecucionPresupuestalProyectosRankSectorRegionLima(years)
    downloader.EjecucionPresupuestalProyectosMes(con,years)
    downloader.Lista_Consulta_Grupo(years,'Funcion')

def tblGirado(years):
    updateTblGirado.main(con,years)

def tblProjects(years):
    addNewProjects.main(con)
    updateSectores.main(con)
    # updateFuente.main(con)
    updateTblPip.main(con,years,1,"FINANCIERO") #Tabla Financiera Act
    # updateTblPip.main(con,years,2,"FINANCIERO") #Tabla Financiera Act

def tblProcompite(years):
    getInfoFinancieraHistoricaProcompiteSOSEM.main(con,years) #Tabla Financiera Act
    # updateTblPip.main(con,years,2)
    # getInfoFinancieraProcompite.main(con)

def updateDiario(years):
    updateTblDiario.main(con,years)

def updateSeguimiento(years):
    updateTblSeguimientoGrupo.main(con,years)
    updateTblSeguimiento.main(con,years)
    verificar_et.main(con,years)

def formato12B(years):
    updateFormato12B.main(con,years)

def tblVias():
    updateTblVias.main(con)

def ranking(years):
    updateTblRankProjects.main(con,years)

def carterapmi():
	addcarterapmi.main(con)

years = datetime.now().year 
fechahoy = datetime.now().date()
if int(years) >= years -1: #Se esta cambiando de 2021 a 2022
    fecha_ult_anio_ant = datetime.strptime(str(years) + "-03-01",'%Y-%m-%d').date()
    anios = [years -1,years]
    for i in anios:
        if i == int(years) - 1: #SE ACTUALIZA EL AÑO ANTERIOR
            if fecha_ult_anio_ant >= fechahoy: #SI LA FECHA ES MAYOR AL PRIMERO DE MARZO
                print("ACTUALIZANDO AÑO <<<<<<<< " + str(i) + " >>>>>>>>>>")
                # dailyDownloads(i)
                # tblGirado(i)
                # updateDiario(i)
                # ranking(i)
                # updateSeguimiento(i)
            else:
                print("!!! AÑO " + str(i) + " SOLO SE ACTUALIZA HASTA EL PRIMERO DE MARZO")
        else:
            print("ACTUALIZANDO AÑO <<<<<<<< " + str(i) + " >>>>>>>>>>") #SE ACTUALIZA EL AÑO ACTUAL
            dailyDownloads(i)
            tblGirado(i)
            updateDiario(i)
            ranking(i)
            updateSeguimiento(i)
            tblVias() #SOLO AÑO ACTUAL
            tblProjects(i) #SOLO AÑO ACTUAL
            formato12B(i)
            # carterapmi()
            # tblProcompite(i) #SOLO AÑO ACTUAL
else:
    print("Año mayor a 2020") 