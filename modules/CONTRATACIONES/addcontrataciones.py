import sys
import os
import json
import urllib.request as urllib2
from importlib import reload
import reader
import time
import requester
from decimal import Decimal
from datetime import datetime
from datetime import timedelta

reload(sys)

def main(con):
	
	def formatear_fecha(fecha):
		return ((datetime.strptime((fecha.split(' ')[0]), "%d/%m/%Y")).strftime("%Y-%m-%d") if fecha is not None and len(fecha.strip()) > 0 else None)

	def texto(text):

		return	str(text.strip()) if text is not None  and len(text.strip()) > 0 else None

	def totalproyectos():

		con.getCur().execute("""
            select distinct trim(cod_unif) as cod_unif from grli_pip_total_girado 
            """)
		return (con.getCur().fetchall())

	def numero(num):

		numero = None
		if num is not None:
			if num == "":
				numero = None
			else:
				numero = int(num)
		else:
			numero = None
		
		return numero
		# return int(num) if num is not None else None

	def dateExists_contrataciones(fecha,anio):

		con.getCur().execute("""SELECT * FROM tb_contrato WHERE fecha = %s and anio = %s """,(fecha,anio))

		if len(con.getCur().fetchall()) > 0 :
			return True
		return False

	def dateExists_procedimiento_seleccion(fecha):

		con.getCur().execute("""SELECT * FROM contratacionesps WHERE fecha_act = %s """,(fecha,))

		if len(con.getCur().fetchall()) > 0 :
			return True
		return False

	def insertprocesodeseleccion(data,fecha):
		total_cont = 0
		for index, row in enumerate(data):
			codigo = row['cod_unif']
			start = time.time()
			data = requester.requestverProcesoContratacionInv(codigo,1)
			
			if data:
				contrataciones = data
				for	items in contrataciones:			
					TIPO = items['TIPO']
					COD_UNICO = items['COD_UNICO']
					COD_CONVOCATORIA = items['COD_CONVOCATORIA']
					FEC_CONVOCATORIA = items['FEC_CONVOCATORIA']
					NUM_CONTRATO =  items['NUM_CONTRATO'].strip() if items['NUM_CONTRATO'] != None else None
					NUM_ITEM = items['NUM_ITEM']
					PROCEDIM_SELEC = items['PROCEDIM_SELEC']
					TIPO_PROCESO = items['TIPO_PROCESO']
					NOMENCLATURA = items['NOMENCLATURA']
					DES_PROCESO = items['DES_PROCESO']
					IND_PAQUETES = items['IND_PAQUETES']
					DES_ITEM = items['DES_ITEM']
					FEC_HOR_CONVOC = items['FEC_HOR_CONVOC']
					ESTADO = items['ESTADO']
					VALOR_REFER = items['VALOR_REFER']
					VALOR_ESTIM = items['VALOR_ESTIM']
					IND_CRONOG = items['IND_CRONOG']
					ETAPA = items['ETAPA']
					FEC_INICIO = items['FEC_INICIO']
					FEC_TERMINO = items['FEC_TERMINO']
					EST_CRONOG = items['EST_CRONOG']
					TIP_CONTR_ASOC = items['TIP_CONTR_ASOC']
					NOM_CONTRATISTA = items['NOM_CONTRATISTA']
					RUC_CONTRATISTA = items['RUC_CONTRATISTA']
					DES_CONTRATO = items['DES_CONTRATO']
					URL_CONTRATO = items['URL_CONTRATO']
					FEC_SUSCRIPCION = items['FEC_SUSCRIPCION']
					MTO_SUSCRIPCION = items['MTO_SUSCRIPCION']
					MTO_TOTAL = items['MTO_TOTAL']
					MTO_ITEM = items['MTO_ITEM']
					IND_CONTRATO = items['IND_CONTRATO']
					NOM_ENTIDAD = items['NOM_ENTIDAD']
					ABREV_ENTIDAD = items['ABREV_ENTIDAD']
					FEC_PUBLICAC = items['FEC_PUBLICAC']
					FEC_REINICIO = items['FEC_REINICIO']
					OBJETO_CONTRATAC = items['OBJETO_CONTRATAC']
					DES_OBJ_CONT = items['DES_OBJ_CONT']
					COD_MONEDA = items['COD_MONEDA']
					DES_MONEDA = items['DES_MONEDA']
					URL_ACCIONES = items['URL_ACCIONES']
					IND_ACCIONES = items['IND_ACCIONES']
					TIPO_FORMATO = items['TIPO_FORMATO']
					MODAL_EJEC = items['MODAL_EJEC']
					NOM_EJECUTORA = items['NOM_EJECUTORA']
					MODALIDAD = items['MODALIDAD']
					ENTIDAD_CONTRATANTE = items['ENTIDAD_CONTRATANTE']
					RUC_ENT_CONTRATANTE = items['RUC_ENT_CONTRATANTE']
					NUM_RUC_POSTOR = items['NUM_RUC_POSTOR']
					MTO_CONTRATADO = items['MTO_CONTRATADO']
					NOM_GANADOR = items['NOM_GANADOR']
					IND_SEACE = items['IND_SEACE']
					IND_PROC_SEL = items['IND_PROC_SEL']
					N_COD_CONTRATO = items['N_COD_CONTRATO']
					con.getCur().execute("""
						INSERT INTO contratacionesPS(
							TIPO, 
							COD_UNICO,
							COD_CONVOCATORIA,
							FEC_CONVOCATORIA,
							NUM_CONTRATO,
							NUM_ITEM,
							PROCEDIM_SELEC,
							TIPO_PROCESO,
							NOMENCLATURA,
							DES_PROCESO,
							IND_PAQUETES,
							DES_ITEM,
							FEC_HOR_CONVOC,
							ESTADO,
							VALOR_REFER,
							VALOR_ESTIM,
							IND_CRONOG,
							ETAPA,
							FEC_INICIO,
							FEC_TERMINO,
							EST_CRONOG,
							TIP_CONTR_ASOC,
							NOM_CONTRATISTA,
							RUC_CONTRATISTA,
							DES_CONTRATO,
							URL_CONTRATO,
							FEC_SUSCRIPCION,
							MTO_SUSCRIPCION,
							MTO_TOTAL,
							MTO_ITEM,
							IND_CONTRATO,
							NOM_ENTIDAD,
							ABREV_ENTIDAD,
							FEC_PUBLICAC,
							FEC_REINICIO,
							OBJETO_CONTRATAC,
							DES_OBJ_CONT,
							COD_MONEDA,
							DES_MONEDA,
							URL_ACCIONES,
							IND_ACCIONES,
							TIPO_FORMATO,
							MODAL_EJEC,
							NOM_EJECUTORA,
							MODALIDAD,
							ENTIDAD_CONTRATANTE,
							RUC_ENT_CONTRATANTE,
							NUM_RUC_POSTOR,
							MTO_CONTRATADO,
							NOM_GANADOR,
							IND_SEACE,
							IND_PROC_SEL,
							fecha_act,
							N_COD_CONTRATO
							) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
						""",
							(
								numero(TIPO), 
								numero(COD_UNICO),
								numero(COD_CONVOCATORIA),
								formatear_fecha(FEC_CONVOCATORIA),
								texto(NUM_CONTRATO),
								numero(NUM_ITEM),
								texto(PROCEDIM_SELEC),
								texto(TIPO_PROCESO),
								texto(NOMENCLATURA),
								texto(DES_PROCESO),
								texto(IND_PAQUETES),
								texto(DES_ITEM),
								formatear_fecha(FEC_HOR_CONVOC),
								texto(ESTADO),
								numero(VALOR_REFER),
								numero(VALOR_ESTIM),
								texto(IND_CRONOG),
								texto(ETAPA),
								formatear_fecha(FEC_INICIO),
								formatear_fecha(FEC_TERMINO),
								texto(EST_CRONOG),
								texto(TIP_CONTR_ASOC),
								texto(NOM_CONTRATISTA),
								texto(RUC_CONTRATISTA),
								texto(DES_CONTRATO),
								texto(URL_CONTRATO),
								formatear_fecha(FEC_SUSCRIPCION),
								Decimal(MTO_SUSCRIPCION),
								Decimal(MTO_TOTAL),
								Decimal(MTO_ITEM),
								texto(IND_CONTRATO),
								texto(NOM_ENTIDAD),
								texto(ABREV_ENTIDAD),
								formatear_fecha(FEC_PUBLICAC),
								formatear_fecha(FEC_REINICIO),
								texto(OBJETO_CONTRATAC),
								texto(DES_OBJ_CONT),
								numero(COD_MONEDA),
								texto(DES_MONEDA),
								texto(URL_ACCIONES),
								texto(IND_ACCIONES),
								texto(TIPO_FORMATO),
								texto(MODAL_EJEC),
								texto(NOM_EJECUTORA),
								texto(MODALIDAD),
								texto(ENTIDAD_CONTRATANTE),
								texto(RUC_ENT_CONTRATANTE),
								texto(NUM_RUC_POSTOR),
								Decimal(MTO_CONTRATADO),
								texto(NOM_GANADOR),
								texto(IND_SEACE),
								texto(IND_PROC_SEL),
								fecha,
								numero(N_COD_CONTRATO)
							)
					)

				end = time.time()
				print(str(index + 1) + ".- Informacion de procedimiento de seleccion Proyecto: " + str(codigo) + " =>  " + str(round((end - start),2)) + " Seg.")
				total_cont += 1
			else:
				print(str(index + 1) + ".- No hay informacion de procedimiento de selecciones => "+ str(codigo))
		con.getConn().commit()
		# print("Total de proyectos : => " + str(lengh(totalproyectos())))

		print("Procedimiento de seleccion Ingresadas (" + str(total_cont) +") a la Fecha: " + fecha)

	def insertcontrataciones(data,fecha,anio):
		total_cont = 0
		for index, row in enumerate(data):
			codigo = row['cod_unif']
			start = time.time()
			data = requester.requestverProcesoContratacionInv(codigo,3)
			
			if data:
				contrataciones = data
				for	items in contrataciones:			

					COD_UNICO = items['CODIGO_UNICO']
					COD_CONVOCATORIA = items['COD_CONVOCATORIA']
					FEC_CONVOCATORIA = items['FEC_CONVOCATORIA']
					OBJETO_CONTRATAC = items['OBJETO_CONTRATAC']
					TIPO_PROCESO = items['TIPO_PROCESO']
					NUMERO_CONVOCATORIA = items['NUMERO_CONVOCATORIA']
					DES_PROCESO = items['DES_PROCESO']
					NUM_ITEM = items['NUM_ITEM']
					DES_ITEM = items['DES_ITEM']
					ES_CONSORCIO = items['ES_CONSORCIO']
					NOM_CONTRATISTA = items['NOM_CONTRATISTA']
					FEC_SUSCRIPCION = items['FEC_SUSCRIPCION']
					NUM_CONTRATO =  items['NUM_CONTRATO'].strip() if items['NUM_CONTRATO'] != None else None
					MTO_TOTAL = items['MTO_TOTAL']
					MTO_ITEM = items['MTO_ITEM']
					URL_CONTRATO = items['URL_CONTRATO']

					con.getCur().execute("""
						INSERT INTO tb_contrato(
							codigo_unico,
							cod_convocatoria,
							fec_convocatoria,
							objeto_contratac,
							tipo_proceso,
							numero_convocatoria,
							des_proceso,
							num_item,
							des_item,
							es_consorcio,
							nom_contratista,
							fec_suscripcion,
							num_contrato,
							mto_total,
							mto_item,
							url_contrato,
							fecha,
							anio
						) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
						""",
						(
							numero(COD_UNICO),
							numero(COD_CONVOCATORIA),
							formatear_fecha(FEC_CONVOCATORIA),
							texto(OBJETO_CONTRATAC),
							texto(TIPO_PROCESO),
							numero(NUMERO_CONVOCATORIA),
							texto(DES_PROCESO),
							numero(NUM_ITEM),
							texto(DES_ITEM),
							texto(ES_CONSORCIO),	
							texto(NOM_CONTRATISTA),	
							formatear_fecha(FEC_SUSCRIPCION),				
							texto(NUM_CONTRATO),
							Decimal(MTO_TOTAL),
							Decimal(MTO_ITEM),
							texto(URL_CONTRATO),
							fecha,
							anio
						)
					)

				end = time.time()
				print(str(index + 1) + ".- Informacion de contratacion Proyecto: " + str(codigo) + " =>  " + str(round((end - start),2)) + " Seg.")
				total_cont += 1
			else:
				print(str(index + 1) + ".- No hay informacion de contrataciones => "+ str(codigo))
		con.getConn().commit()
		# print("Total de proyectos : => " + str(lengh(totalproyectos())))

		print("Contrataciones Ingresadas (" + str(total_cont) +") a la Fecha: " + fecha)

	def __init__():

		fecha = datetime.now()
		anio = datetime.now().year 
		print("******************** INICIO DE PROCESO DE SELLECION ****************************")
		if dateExists_procedimiento_seleccion(fecha.date()):
			print('Ya existen registros con la fecha [' + str(fecha.date()) + ']' )
		else:
			data = totalproyectos()
			insertprocesodeseleccion(data,str(fecha.date()))
		print("******************** FIN DE PROCESO DE SELLECION ****************************")

		print("******************** INICIO DE CONTRATACIONES ****************************")
		if dateExists_contrataciones(fecha.date(),anio):
			print('Ya existen registros con la fecha [' + str(fecha.date()) + ']' )
		else:
			data = totalproyectos()
			insertcontrataciones(data,str(fecha.date()),anio)
		print("******************** FIN DE CONTRATACIONES ****************************")

	# Iniciar
	__init__()