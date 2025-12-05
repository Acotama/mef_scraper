import sys
import os
import json
import urllib.request as urllib2
from importlib import reload
import reader
import time
import requester
import unicodedata
import numpy as np
from decimal import Decimal
from datetime import datetime
from datetime import timedelta

reload(sys)

def main(con):
    
    #array_funcion
    arrayfuncion = {
        1:"LEGISLATIVA",
        2:"RELACIONES EXTERIORES",
        3:"PLANEAMIENTO, GESTION Y RESERVA DE CONTINGENCIA",
        4:"DEFENSA Y SEGURIDAD NACIONAL",
        5:"ORDEN PUBLICO Y SEGURIDAD",
        6:"JUSTICIA",
        7:"TRABAJO",
        8:"COMERCIO",
        9:"TURISMO",
        10:"AGROPECUARIA",
        11:"PESCA",
        12:"ENERGIA",
        13:"MINERIA",
        14:"INDUSTRIA",
        15:"TRANSPORTE",
        16:"COMUNICACIONES",
        17:"AMBIENTE",
        18:"SANEAMIENTO",
        19:"VIVIENDA Y DESARROLLO URBANO",
        20:"SALUD",
        21:"CULTURA Y DEPORTE",
        22:"EDUCACION",
        23:"PROTECCION SOCIAL",
        24:"PREVISION SOCIAL",
        25:"DEUDA PUBLICA",
        None:"SIN DATA"
    }

    #array_programa
    arrayprograma = {
        "001": "ACCION LEGISLATIVA",
        "002": "SERVICIO DIPLOMATICO",
        "003": "COOPERACION INTERNACIONAL",
        "004": "PLANEAMIENTO GUBERNAMENTAL",
        "005": "INFORMACION PUBLICA",
        "006": "GESTION",
        "007": "RECAUDACION",
        "008": "RESERVA DE CONTINGENCIA",
        "009": "CIENCIA Y TECNOLOGIA",
        "010": "EFICIENCIA DE MERCADOS",
        "011": "TRANSFERENCIAS E INTERMEDIACION FINANCIERA",
        "012": "IDENTIDAD Y CIUDADANIA",
        "013": "DEFENSA Y SEGURIDAD NACIONAL",
        "014": "ORDEN INTERNO",
        "015": "CONTROL DE DROGAS",
        "016": "GESTION DE RIESGOS Y EMERGENCIAS",
        "017": "ADMINISTRACION DE JUSTICIA",
        "018": "SEGURIDAD JURIDICA",
        "019": "READAPTACION SOCIAL",
        "020": "TRABAJO",
        "021": "COMERCIO",
        "022": "TURISMO",
        "023": "AGRARIO",
        "024": "PECUARIO",
        "025": "RIEGO",
        "026": "PESCA",
        "027": "ACUICULTURA",
        "028": "ENERGIA ELECTRICA",
        "029": "HIDROCARBUROS",
        "030": "MINERIA",
        "031": "INDUSTRIA",
        "032": "TRANSPORTE AEREO",
        "033": "TRANSPORTE TERRESTRE",
        "034": "TRANSPORTE FERROVIARIO",
        "035": "TRANSPORTE HIDROVIARIO",
        "036": "TRANSPORTE URBANO",
        "037": "COMUNICACIONES POSTALES",
        "038": "TELECOMUNICACIONES",
        "054": "DESARROLLO ESTRATEGICO, CONSERVACION Y APROVECHAMIENTO SOSTENIBLE DEL PATRIMONIO NATURAL",
        "055": "GESTION INTEGRAL DE LA CALIDAD AMBIENTAL",
        "040": "SANEAMIENTO",
        "041": "DESARROLLO URBANO",
        "042": "VIVIENDA",
        "043": "SALUD COLECTIVA",
        "044": "SALUD INDIVIDUAL",
        "045": "CULTURA",
        "046": "DEPORTES",
        "047": "EDUCACION BASICA",
        "048": "EDUCACION SUPERIOR",
        "049": "DUCACION TECNICA PRODUCTIVA",
        "050": "ASISTENCIA EDUCATIVA",
        "051": "ASISTENCIA SOCIAL",
        "052": "PREVISION SOCIAL",
        "053": "DEUDA PUBLICA",
        None: "SIN DATA"
    }
    #array subprograma
    arraysubprograma = {
        "0001": "ACCION LEGISLATIVA",
        "0002": "SERVICIO DIPLOMATICO",
        "0003": "COOPERACION INTERNACIONAL",
        "0004": "RECTORIA DE SISTEMAS ADMINISTRATIVOS",
        "0005": "PLANEAMIENTO INSTITUCIONAL",
        "0006": "INFORMACION PUBLICA",
        "0007": "DIRECCION Y SUPERVISION SUPERIOR",
        "0008": "ASESORAMIENTO Y APOYO",
        "0009": "SOPORTE TECNOLOGICO",
        "0010": "INFRAESTRUCTURA Y EQUIPAMIENTO",
        "0011": "REPARACION Y PERFECCIONAMIENTO DE RECURSOS HUMANOS",
        "0012": "CONTROL INTERNO",
        "0013": "RECAUDACION",
        "0014": "RESERVA DE CONTINGENCIA",
        "0015": "INVESTIGACION BASICA",
        "0016": "INVESTIGACION APLICADA",
        "0017": "INNOVACION TECNOLOGICA",
        "0018": "FICIENCIA DEMERCADOS",
        "0019": "TRANSFERENCIAS DE CARACTER GENERAL",
        "0020": "INTERMEDIACION FINANCIERA",
        "0021": "REGISTROS CIVILES E IDENTIFICACION",
        "0022": "REGISTROS PUBLICOS",
        "0023": "DEFENSA DEL INTERES CIUDADANO",
        "0024": "LECCIONES,REFERENDOS Y CONSULTAS CIUDADANAS",
        "0025": "JUSTICIA ELECTORAL",
        "0026": "DEFENSA NACIONAL",
        "0027": "SEGURIDAD NACIONAL",
        "0028": "OPERACIONES POLICIALES",
        "0029": "CONTROL MIGRATORIO",
        "0030": "CONTROL DE ARMAS, MUNICIONES, EXPLOSIVOS DE USO CIVIL Y SERVICIOS DE SEGURIDAD",
        "0031": "SEGURIDAD VECINAL Y COMUNAL",
        "0032": "DESARROLLO ALTERNATIVO",
        "0033": "PREVENCION Y REHABILITACION",
        "0034": "INTERDICCION, LAVADO DE DINERO Y DELITOS CONEXOS",
        "0035": "PREVENCION DE DESASTRES",
        "0036": "ATENCION INMEDIATA DE DESASTRES",
        "0037": "DEFENSA CONTRA INCENDIOS Y EMERGENCIAS MENORES",
        "0038": "ADMINISTRACION DE JUSTICIA",
        "0039": "DEFENSA DE LOS DERECHOS CONSTITUCIONALES Y LEGALES",
        "0040": "READAPTACION SOCIAL",
        "0041": "REGULACION Y CONTROL DE LA RELACION LABORAL",
        "0042": "PROMOCION LABORAL",
        "0043": "PROMOCION DEL COMERCIO INTERNO",
        "0044": "PROMOCION DEL COMERCIO EXTERNO",
        "0045": "PROMOCION DEL TURISMO",
        "0046": "PROTECCION SANITARIA VEGETAL",
        "0047": "INOCUIDAD AGROALIMENTARIA",
        "0048": "PROTECCION SANITARIA ANIMAL",
        "0049": "INOCUIDAD PECUARIA",
        "0050": "INFRAESTRUCTURA DE RIEGO",
        "0051": "RIEGO TECNIFICADO",
        "0052": "REGULACION Y ADMINISTRACION DEL RECURSO ICTIOLOGICO",
        "0053": "INFRAESTRUCTURA PESQUERA",
        "0054": "FOMENTO DE LA PRODUCCION ACUICOLA",
        "0055": "GENERACION DE ENERGIA ELECTRICA",
        "0056": "TRANSMISION DE ENERGIA ELECTRICA",
        "0057": "DISTRIBUCION DE ENERGIA ELECTRICA",
        "0058": "HIDROCARBUROS",
        "0059": "PROMOCION MINERA",
        "0060": "PROMOCION DE LA INDUSTRIA",
        "0061": "INFRAESTRUCTURA AEROPORTUARIA",
        "0062": "CONTROL Y SEGURIDAD DEL TRAFICO AEREO",
        "0063": "SERVICIOS DE TRANSPORTE AEREO",
        "0064": "VIAS NACIONALES",
        "0065": "VIAS DEPARTAMENTALES",
        "0066": "VIAS VECINALES",
        "0067": "CAMINOS DE HERRADURA",
        "0068": "CONTROL Y SEGURIDAD DEL TRAFICO TERRESTRE",
        "0069": "SERVICIOS DE TRANSPORTE TERRESTRE",
        "0070": "FERROVIAS",
        "0071": "PUERTOS Y TERMINALES FLUVIALES Y LACUSTRES",
        "0072": "CONTROL Y SEGURIDAD DEL TRAFICO HIDROVIARIO",
        "0073": "SERVICIOS DE TRANSPORTE HIDROVIARIO",
        "0074": "VIAS URBANAS",
        "0075": "CONTROL Y SEGURIDAD DEL TRAFICO URBANO",
        "0076": "SERVICIOS DE TRANSPORTE URBANO",
        "0077": "SERVICIOS POSTALES",
        "0078": "SERVICIOS DE TELECOMUNICACIONES",
        "0079": "GESTION DEL ESPACIO ELECTROMAGNETICO",
        "0088": "SANEAMIENTO URBANO",
        "0089": "SANEAMIENTO RURAL",
        "0090": "PLANEAMIENTO Y DESARROLLO URBANO Y RURAL",
        "0091": "VIVIENDA",
        "0092": "CONSTRUCCION",
        "0093": "REGULACION Y CONTROL SANITARIO",
        "0094": "CONTROL EPIDEMIOLOGICO",
        "0095": "CONTROL DE RIESGOS Y DANOS PARA LA SALUD",
        "0096": "ATENCION MEDICA BASICA",
        "0097": "ATENCION MEDICA ESPECIALIZADA",
        "0098": "SERVICIOS DE DIAGNOSTICO Y TRATAMIENTO",
        "0099": "PATRIMONIO HISTORICO Y CULTURAL",
        "0100": "PROMOCION Y DESARROLLO CULTURAL",
        "0101": "PROMOCION Y DESARROLLO DEPORTIVO",
        "0102": "INFRAESTRUCTURA DEPORTIVA Y RECREATIVA",
        "0103": "EDUCACION INICIAL",
        "0104": "EDUCACION PRIMARIA",
        "0105": "EDUCACION SECUNDARIA",
        "0106": "EDUCACION BASICA ALTERNATIVA",
        "0107": "EDUCACION BASICA ESPECIAL",
        "0108": "EDUCACION SUPERIOR NO UNIVERSITARIA",
        "0109": "EDUCACION SUPERIOR UNIVERSITARIA",
        "0110": "EDUCACION DE POST- GRADO",
        "0111": "EXTENSION UNIVERSITARIA",
        "0112": "FORMACION OCUPACIONAL",
        "0113": "BECAS Y CREDITOS EDUCATIVOS",
        "0114": "DESARROLLO DE CAPACIDADES SOCIALES Y ECONOMICAS",
        "0115": "PROTECCION DE POBLACIONES EN RIESGO",
        "0116": "SISTEMAS DE PENSIONES",
        "0117": "SEGURIDAD SOCIAL EN SALUD",
        "0118": "PAGO DE LA DEUDA PUBLICA",
        "0119": "CONSERVACION Y APROVECHAMIENTO SOSTENIBLE DE LA DIVERSIDAD BIOLOGICA Y DE LOS RECURSOS NATURAL",
        "0120": "GESTION INTEGRADA Y SOSTENIBLE DE LOS ECOSISTEMAS",
        "0121": "GESTION DEL CAMBIO CLIMATICO",
        "0122": "GESTION INTEGRADA DE LOS RECURSOS HIDRICOS",
        "0123": "GESTION DEL TERRITORIO",
        "0124": "GESTION DE LOS RESIDUOS SOLIDOS",
        "0125": "CONSERVACION Y AMPLIACION DE LAS AREAS VERDES Y ORNATO PUBLICO",
        "0126": "VIGILANCIA Y CONTROL INTEGRAL DE LA CONTAMINACION Y REMEDIACION AMBIENTAL",
        "0127": "CONTROL INTEGRAL DE SUSTANCIAS QUIMICAS Y MATERIALES PELIGROSOS",
        "0128": "DESARROLLO EXPERIMENTAL",
        "0129": "TRANSFERENCIA DE CONOCIMIENTOS Y TECNOLOGIAS",
        "0142": "CONSTRUCCION Y MEJORAMIENTO DE CARRETERAS",
        None: "SIN DATA"
    }


    # Función para normalizar texto
    def normalize(text):
        if text is None:
            return None
        return ''.join(
            char for char in unicodedata.normalize('NFD', text)
            if unicodedata.category(char) != 'Mn'
        ).upper()  # Cambia a .lower() si quieres todo en minúsculas
    
    def formatear_fecha(fecha):
        return ((datetime.strptime((fecha.split(' ')[0]), "%d/%m/%Y")).strftime("%Y-%m-%d") if fecha is not None and len(fecha.strip()) > 0 else None)

    def texto(text):
        return	str(text.strip()) if text is not None  and len(text.strip()) > 0 else None
    
    def numero(num):	
        return int(num) if num is not None else None

    def dateExists_carterapmi(fecha):
        con.getCur().execute("""SELECT * FROM cartera_pmi WHERE fecha_act = %s and anio = %s""",(fecha,fecha.year))
        if len(con.getCur().fetchall()) > 0 :
            return True
        return False

    def array_brechas(data):
        array_b = '{}'
        print("Consultando Brechas")

        with open('data.json', 'r') as file:
            data_b = json.load(file)

        if data:
            carterapmi = data
            z = json.loads(array_b) 
            for	items in carterapmi:
                CODIGO_UNICO = items["CODIGO_UNICO"]
                if CODIGO_UNICO is not None:
                    print("CODIGO PROYECTO: [" + str(CODIGO_UNICO) + "]")
                    if str(CODIGO_UNICO) not  in data_b :
                        data = requester.brechas(str(CODIGO_UNICO))
                        if len(data) > 0:
                            y = {CODIGO_UNICO : [data['BRECHA'],data['INDICADOR'],data['UNIDAD_MEDIDA'],data['ESPACIO_GEO'],data['CONT_CIERRE']]} 
                            z.update(y) 
                    else:
                        y = {CODIGO_UNICO : [data_b[str(CODIGO_UNICO)][0],data_b[str(CODIGO_UNICO)][1],data_b[str(CODIGO_UNICO)][2],data_b[str(CODIGO_UNICO)][3],data_b[str(CODIGO_UNICO)][4]]} 
                        z.update(y) 
                    with open('data.json', 'w') as outfile:
                        json.dump(z, outfile)
        with open('data.json', 'r') as file:
            brechas = json.load(file)
        
        return brechas

    def insertarcarterapmi(fecha):
        data = requester.requestverCarteradeInversiones(None)
        array_brecha = array_brechas(data)
        total_cont = 0
        if data:
            carterapmi = data
            for	index, items in enumerate(carterapmi):
                ID_CARTERA_INVERSION = items["ID_CARTERA_INVERSION"]
                ANO_EJE = items["ANO_EJE"]
                SEC_EJEC = items["SEC_EJEC"]
                TIPO_INVERSION = items["TIPO_INVERSION"]
                CICLO_INVERSION = items["CICLO_INVERSION"]
                CODIGO_UNICO = items["CODIGO_UNICO"]
                ESTADO = items["ESTADO"]
                TIPOLOGIA_PROYECTO = items["TIPOLOGIA_PROYECTO"]
                NATURALEZA = items["NATURALEZA"]
                NOMBRE_INVERSION = items["NOMBRE_INVERSION"]
                ID_TIPOGOBIERNO = items["ID_TIPOGOBIERNO"]
                ID_DEPARTAMENTO = items["ID_DEPARTAMENTO"]
                ID_PROVINCIA = items["ID_PROVINCIA"]
                ID_DISTRITO = items["ID_DISTRITO"]
                ID_FUNCION = items["ID_FUNCION"]
                ID_DIVISION = items["ID_DIVISION"]
                ID_GRUPO = items["ID_GRUPO"]
                ID_PROGRAMA = items["ID_PROGRAMA"]
                ID_SUB_PROGRAMA = items["ID_SUB_PROGRAMA"]
                COSTO = items["COSTO"]
                DEVENGADO_ACUMULADO = items["DEVENGADO_ACUMULADO"]
                FUENTE_FINANC = items["FUENTE_FINANC"]
                MODALIDAD_EJECUCION = items["MODALIDAD_EJECUCION"]
                INICIO_FORMULACION = items["INICIO_FORMULACION"]
                FIN_FORMULACION = items["FIN_FORMULACION"]
                INICIO_EJECUCION = items["INICIO_EJECUCION"]
                FIN_EJECUCION = items["FIN_EJECUCION"]
                PROGRAMACION_INVERSION_ANIO0 = items["PROGRAMACION_INVERSION_ANIO0"]
                PROGRAMACION_INVERSION_ANIO1 = items["PROGRAMACION_INVERSION_ANIO1"]
                PROGRAMACION_INVERSION_ANIO2 = items["PROGRAMACION_INVERSION_ANIO2"]
                PROGRAMACION_INVERSION_ANIO3 = items["PROGRAMACION_INVERSION_ANIO3"]
                ID_BRECHA = items["ID_BRECHA"]
                PROGRAMA_PPTO = items["PROGRAMA_PPTO"]
                META_FISICA_NOMBRE = items["META_FISICA_NOMBRE"]
                META_FISICA_ANO1 = items["META_FISICA_ANO1"]
                META_FISICA_ANO2 = items["META_FISICA_ANO2"]
                META_FISICA_ANO3 = items["META_FISICA_ANO3"]
                ESTADO_REG = items["ESTADO_REG"]
                USUARIO_CREA = items["USUARIO_CREA"]
                FECHA_CREA = items["FECHA_CREA"]
                USUARIO_MODIFICA = items["USUARIO_MODIFICA"]
                FECHA_MODIFICA = items["FECHA_MODIFICA"]
                ID_UNIDADMEDIDA = items["ID_UNIDADMEDIDA"]
                ID_RUBRO = items["ID_RUBRO"]
                PRIORIDAD = items["PRIORIDAD"]
                ID_UNIDAD = items["ID_UNIDAD"]
                TIPO_PROYECTO = items["TIPO_PROYECTO"]
                PIM = items["PIM"]
                ID_RUBRO_2 = items["ID_RUBRO_2"]
                FUENTE_FINANC_2 = items["FUENTE_FINANC_2"]
                ID_SERVICIO = items["ID_SERVICIO"]
                CODIGO_PROYECTO = items["CODIGO_PROYECTO"]
                CATEG_PROYECTO = items["CATEG_PROYECTO"]
                ES_TRANSFERENCIA = items["ES_TRANSFERENCIA"]
                COD_PROGRAMA = items["COD_PROGRAMA"]
                COD_IDEA = items["COD_IDEA"]
                FECHA_REGISTRO = items["FECHA_REGISTRO"]
                FECHA_VIABILIDAD = items["FECHA_VIABILIDAD"]
                ID_UNIDAD_UEI = items["ID_UNIDAD_UEI"]
                DES_UNIDAD_UEI = items["DES_UNIDAD_UEI"]
                ID_USUARIO_UEI = items["ID_USUARIO_UEI"]
                NOMBRE_EJECUTORA = items["NOMBRE_EJECUTORA"]
                NOMBRE_GOBIERNO = items["NOMBRE_GOBIERNO"]
                NOMBRE_FUENTE = items["NOMBRE_FUENTE"]
                NOMBRE_CICLOINVERSION = items["NOMBRE_CICLOINVERSION"]
                NOMBRE_TIPOINVERSION = items["NOMBRE_TIPOINVERSION"]
                NOMBRE_FUNCION = normalize(items["NOMBRE_FUNCION"])
                NOMBRE_DIVISION = normalize(items["NOMBRE_DIVISION"])
                NOMBRE_GRUPO = normalize(items["NOMBRE_GRUPO"])
                NOMBRE_DEPARTAMENTO = items["NOMBRE_DEPARTAMENTO"]
                NOMBRE_PROVINCIA = items["NOMBRE_PROVINCIA"]
                NOMBRE_DISTRITO = items["NOMBRE_DISTRITO"]
                NOMBRE_SECTOR_REGISTRO = items["NOMBRE_SECTOR_REGISTRO"]
                NOMBRE_ENTIDAD = items["NOMBRE_ENTIDAD"]
                DES_TIPOLOGIA = items["DES_TIPOLOGIA"]
                DES_SERVICIO = items["DES_SERVICIO"]
                DES_BRECHA = items["DES_BRECHA"]
                ID_CONVENIO = items["ID_CONVENIO"]
                SALDO_PROGRAMAR = items["SALDO_PROGRAMAR"]
                SALDO_PROGRAMAR_SIN_PIM = items["SALDO_PROGRAMAR_SIN_PIM"]
                FECHA_REGISTRO_STR = items["FECHA_REGISTRO_STR"]
                FECHA_VIABILIDAD_STR = items["FECHA_VIABILIDAD_STR"]
                INICIO_EJECUCION_STR = items["INICIO_EJECUCION_STR"]
                FIN_EJECUCION_STR = items["FIN_EJECUCION_STR"]
                ES_EMPRESA = items["ES_EMPRESA"]
                TIPO_EMPRESA = items["TIPO_EMPRESA"]
                ID_IDENTIFICA = items["ID_IDENTIFICA"]
                ID_TIPO_FORMATO = items["ID_TIPO_FORMATO"]
                ID_SUB_PROGRAMA_SECTOR = items["ID_SUB_PROGRAMA_SECTOR"]
                COD_UBIGEO_UEP = items["COD_UBIGEO_UEP"]
                DES_UND_PRESUPUESTAL = items["DES_UND_PRESUPUESTAL"]
                DES_PLIEGO_PRESUPUESTAL = items["DES_PLIEGO_PRESUPUESTAL"]
                TIENE_COEJECUCION = items["TIENE_COEJECUCION"]
                COD_PLIEGO = items["COD_PLIEGO"]
                NRO_DOC_SUSTENTO = items["NRO_DOC_SUSTENTO"]
                ID_ARCHIVO = items["ID_ARCHIVO"]
                TIPO_NO_PREVISTA = items["TIPO_NO_PREVISTA"]
                URL_ARCHIVO = items["URL_ARCHIVO"]
                NOMBRE_ARCHIVO = items["NOMBRE_ARCHIVO"]
                listaModalidad = items["listaModalidad"]
                listaFuente = items["listaFuente"]
                DES_TIPO_NO_PREVISTA = items["DES_TIPO_NO_PREVISTA"]
                FECHA_MODIFICA_STR = items["FECHA_MODIFICA_STR"]
                ES_CONTINUIDAD = items["ES_CONTINUIDAD"]
                ES_EXCEPCIONAL = items["ES_EXCEPCIONAL"]
                PROGRAMACION_INV_ANIO_ANT = items["PROGRAMACION_INV_ANIO_ANT"]
                ES_CARTERA = items["ES_CARTERA"]
                DEVENGADO_ACUMULADO_ACTUAL = items["DEVENGADO_ACUMULADO_ACTUAL"]
                SALDO_PROGRAMABLE = items["SALDO_PROGRAMABLE"]
                PROGRAMACION_INVERSION_ANIO0_ANT = items["PROGRAMACION_INVERSION_ANIO0_ANT"]
                PROGRAMACION_INVERSION_ANIO1_ANT = items["PROGRAMACION_INVERSION_ANIO1_ANT"]
                PROGRAMACION_INVERSION_ANIO2_ANT = items["PROGRAMACION_INVERSION_ANIO2_ANT"]
                PROGRAMACION_INVERSION_ANIO3_ANT = items["PROGRAMACION_INVERSION_ANIO3_ANT"]
                SALDO_PENDIENTE = items["SALDO_PENDIENTE"]
                PUNTAJE = items["PUNTAJE"]
                COD_PROGRAMA_ASOCIADO = items["COD_PROGRAMA_ASOCIADO"]
                ES_COFINANCIADO = items["ES_COFINANCIADO"]
                ES_ELIMINABLE = items["ES_ELIMINABLE"]
                CONDICION_INVERSION = items["CONDICION_INVERSION"]
                SALDO_PROGRAMABLE_2 = items["SALDO_PROGRAMABLE_2"]
                DEVENGADO_ACTUAL = items["DEVENGADO_ACTUAL"]
                CERTIFICADO_ACTUAL = items["CERTIFICADO_ACTUAL"]
                ES_CARTERA_PROG = items["ES_CARTERA_PROG"]
                ID_DOC_TECNICO = items["ID_DOC_TECNICO"]
                ESTADO_INVERSION = items["ESTADO_INVERSION"]
                MONTO_VIABLE = items["MONTO_VIABLE"]
                NOMBRE_PROGRAMA_PPTO = items["NOMBRE_PROGRAMA_PPTO"]
                NOMBRE_FUENTEFINAN = items["NOMBRE_FUENTEFINAN"]
                NOMBRE_MODALIDAD = items["NOMBRE_MODALIDAD"]
                DES_UNIDAD_UF = items["DES_UNIDAD_UF"]
                ES_CADENA_INCORRECTA = items["ES_CADENA_INCORRECTA"]
                ID_DOC_OPERACIONES_CREDITO = items["ID_DOC_OPERACIONES_CREDITO"]
                URL_ARCHIVO_OPER_CREDITO = items["URL_ARCHIVO_OPER_CREDITO"]
                NOMBRE_ARCHIVO_OPER_CREDITO = items["NOMBRE_ARCHIVO_OPER_CREDITO"]
                ES_CONSISTENCIA = items["ES_CONSISTENCIA"]
                FECHA_NO_PREVISTA = items["FECHA_NO_PREVISTA"]
                FECHA_NO_PREVISTA_STR = items["FECHA_NO_PREVISTA_STR"]
                FECHA_ULT_F12B = items["FECHA_ULT_F12B"]
                FECHA_ULT_F12B_STR = items["FECHA_ULT_F12B_STR"]

                # F8
                BRECHA = array_brecha[str(CODIGO_UNICO)][0] if str(CODIGO_UNICO) in array_brecha else None
                INDICADOR = array_brecha[str(CODIGO_UNICO)][1] if str(CODIGO_UNICO) in array_brecha else None
                UNIDAD_MEDIDA = array_brecha[str(CODIGO_UNICO)][2] if str(CODIGO_UNICO) in array_brecha else None
                ESPACIO_GEO = array_brecha[str(CODIGO_UNICO)][3] if str(CODIGO_UNICO) in array_brecha else None
                CONT_CIERRE = array_brecha[str(CODIGO_UNICO)][4] if str(CODIGO_UNICO) in array_brecha else None
                                
                # print(indicador)
        
                # print(str(ID_GRUPO))
                print(str(index + 1) + ".- INGRESANDO CARTERA PMI: ==>  CODIGO PROYECTO: [" + str(CODIGO_UNICO) + "] , CODIGO IDEA: ["+ str(COD_IDEA) +"]")
                con.getCur().execute("""
                    INSERT INTO cartera_pmi(
                        prioridad,
                        orden_prelacion,
                        sector,
                        opmi,
                        codigo_unico,
                        codigo_idea,
                        tipo_inversion,
                        nombre_inversion,
                        funcion,
                        programa,
                        subprograma,
                        costo_actualizado,
                        devengado_acumulado,
                        pim,
                        monto_anio_0,
                        monto_anio_1,
                        monto_anio_2,
                        monto_anio_3,
                        fecha_act,
                        anio,provincia,distrito,brecha,indicador,unidad_medida,espacio_geo,cont_cierre) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        numero(PRIORIDAD), 
                        texto(CONDICION_INVERSION),
                        texto(NOMBRE_SECTOR_REGISTRO),
                        texto(NOMBRE_ENTIDAD),
                        numero(CODIGO_UNICO),
                        numero(COD_IDEA),
                        texto(NOMBRE_TIPOINVERSION),
                        texto(NOMBRE_INVERSION),
                        arrayfuncion[ID_FUNCION] if arrayfuncion.get(ID_FUNCION) != "SIN DATA" else NOMBRE_FUNCION,
                        arrayprograma[ID_DIVISION] if arrayprograma.get(ID_DIVISION) != "SIN DATA" else NOMBRE_DIVISION,
                        arraysubprograma[ID_GRUPO] if arraysubprograma.get(ID_GRUPO) != "SIN DATA" else NOMBRE_GRUPO,
                        numero(COSTO),
                        numero(DEVENGADO_ACUMULADO),
                        numero(PIM),
                        numero(PROGRAMACION_INVERSION_ANIO0),
                        numero(PROGRAMACION_INVERSION_ANIO1),
                        numero(PROGRAMACION_INVERSION_ANIO2),
                        numero(PROGRAMACION_INVERSION_ANIO3),
                        fecha.date(),
                        fecha.year,
                        texto(NOMBRE_PROVINCIA),
                        texto(NOMBRE_DISTRITO),
                        texto(BRECHA),
                        texto(INDICADOR),
                        texto(UNIDAD_MEDIDA),
                        texto(ESPACIO_GEO),
                        texto(CONT_CIERRE)
                    )
                )
                print("GUARDADO")
                total_cont += 1
            print("CARTERA PMI INGRESADAS (" + str(total_cont) +") A LA FECHA: " + str(fecha.date()))
        else:
            print("NO HAY INFORMACIÓN DE CARTERA PMI")
            
    
    def __init__():
        fecha = datetime.now()
        print("******************** INICIO CARTERA DE PMI ****************************")
        # data = requester.brechas('2288164')
        # indicador = None
        # if len(data) > 0:
        # 	indicador = data['INDICADOR']
        # print(indicador)

        if dateExists_carterapmi(fecha.date()):
            print('YA EXISTE REGISTRO CON LA FECHA [' + str(fecha.date()) + ']' )
        else:
            insertarcarterapmi(fecha)
            con.getConn().commit()
        print("******************** FIN CARTERA DE PMI ****************************")
    # Iniciar
    __init__()

    

