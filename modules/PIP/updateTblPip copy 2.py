import sys
import requester
from decimal import Decimal
from commons import twoDec
from importlib import reload
from datetime import datetime
import time
import json
reload(sys)

def main(con,years,opc):

    unidadesEjecutoras = {
        1027: 'SEDE CENTRAL REGION LIMA',
        1325: 'GERENCIA SUB REGIONAL DE LIMA SUR',  # SGRLS
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

    # with open('./downloads//'+ str(years) +'//EjecucionPresupuestal/ProyectosJSON/DataInv.json') as file:
    #     dataJSON = json.load(file)

    def getProjects():
        # select cod_unif from vw_grli_pip_seguimiento_ejecucion_financiera
        # and (grli_pip_total_priori.fecha is null or  grli_pip_total_priori.fecha != %s) 
        # str(datetime.now().date())
        if(opc == 1):
            con.getCur().execute("""
                SELECT
                    cod_snip,
                    grli_pip_total_priori.cod_unif,
                    grli_pip_total_priori.id,grli_pip_total_priori.ger_direc,tipo_pry,tipo_inversion,cierre,tipo_emergencia,case when dif_dev_dia is null then 0 else dif_dev_dia end as dif_dev_dia,exptec_pdf ,f_exptec 
                FROM grli_pip_total_priori
                left join (select codigo ,inf_financiera2.dev_dia as  dif_dev_dia from  inf_financiera2
              		where inf_financiera2.fecha =(select max(fecha) from inf_financiera2) and mes=%s) as p on grli_pip_total_priori.cod_unif = p.codigo 
                WHERE  grli_pip_total_priori.cod_unif in (select cod_unif from vw_grli_pip_seguimiento_ejecucion_financiera) 
                order by ger_direc desc
            """,(str(datetime.now().month),)) 
        else:
            con.getCur().execute("""
            SELECT
                    cod_snip,
                    cod_unif,
                    id,ger_direc,tipo_pry,tipo_inversion,cierre,tipo_emergencia,exptec_pdf ,f_exptec 
                FROM grli_pip_total_priori
                WHERE (id <> 1000)   ORDER BY anio desc
            """,(str(datetime.now().date()),))
        #  WHERE (id <> 1000) ORDER BY anio desc;
        # and cod_unif ='2245361'

        return con.getCur().fetchall()

    def dateExists(cod_unif):
        con.getCur().execute("""
            SELECT * FROM grli_pip_total_priori
            WHERE cod_unif = %s 
            """, (cod_unif,))    

        if len(con.getCur().fetchall()) > 0 :
            return True
        return False

    def etapa_inversion(codigo_unico,_situacion,_dev_a,_costo_a,_cierre,f8,f12b):
        form_eva = _situacion
        avance =   round((_dev_a/_costo_a)*100,2) if _costo_a != 0 else 0
        cierre = _cierre

        # mydata =  requestFormato08(codigo)
        # expediente_tecnico = data_et(codigo,mydata)
        _etapa_f8 =  f8 if f8 is not None else "None"

        if _etapa_f8.find("(C)") != -1 or _etapa_f8.find("(B)") != -1 :
            data = _etapa_f8.split()[2]
            seccion = data[1:2]
            if seccion == "C":
                seccion = "EJECUCION FISICA"
            elif seccion == "B":
                seccion = "EXPEDIENTE TECNICO"
        else:
            seccion = None

        data_f12b = f12b
        situacion = None
        problematica = None
        for row in data_f12b:
            situacion = row["ULT_ESTADO_SITUACIONAL"]
            problematica = row["ULT_PROBLEMA"]

        situacion = situacion.upper() if situacion is not None else "None"
        problematica =  problematica.upper() if problematica is not None else "None"

        if situacion.find("PARALI") != -1 or situacion.find("PARALIZADO") != -1 or situacion.find("PARALIZADA") != -1:
            paralizado = "PARALIZADO"
        elif problematica.find("PARALI") != -1 or problematica.find("PARALIZADO") != -1 or problematica.find("PARALIZADA") != -1:
            paralizado = "PARALIZADO"
        else:
            paralizado = None

        if avance >= 95.00 :
            culminado = "CULMINADO"
        else:
            culminado = None

        if cierre.upper() == "NO" or cierre.upper() == "NO CULMINADA":
            cierre = None
        elif cierre.upper() == "SÍ, EN PROCESO DE LIQUIDACIÓN":
            cierre = "EN PROCESO DE LIQUIDACION"
        elif cierre.upper() == "SÍ, CON LIQUIDACIÓN":
            cierre = "CERRADO"
        # print(data_f12b)
        # print("========================================")
        # print(form_eva)
        # print(seccion)
        # print(paralizado)
        # print(culminado)
        # print(cierre)

        # print("=================== ETAPA =============")

        if cierre is not None:
            etapa = cierre
        elif  culminado is not None:
            etapa = culminado
        elif  paralizado is not None:
            etapa = paralizado
        elif  seccion is not None:
            etapa = seccion
        elif  form_eva is not None:
            etapa = form_eva
        else:
            etapa = None

        return etapa

    # Datos del SSI
    def updateWithInfoSnip(infoSnipdata, codigoSnip, codigoUnif, idProyecto,data):
        for infoSnip in infoSnipdata:
            _nom_proyec       =   ""
            _cod_snip         =   codigoSnip
            _cod_unif         =   codigoUnif
            _u_formul         =   ""
            _sector           =   ""
            _m_pip            =   Decimal(0.00)
            _m_viab           =   Decimal(0.00)
            _m_exptec         =   Decimal(0.00)
            _est_pry          =   ""
            _progr            =   ""
            _subprogr         =   ""
            _poretapas        =   0
            _beneficiarios    =   ""
            _ult_anio_ejec_pry_financ = 0
            _marcas           = ""
            _marco            = ""
            _por_etapas       = "" #FlagEtapas
            _informe_cierre   = "" #FlagCerrado
            _expediente_tecnico= ""
            _fecha_registro_str = None
            _a_exptec = None
            _m_fianza = 0
            _m_laudo = 0
            _mes_ano_pri_dev= ""
            _mes_ano_ult_dev= ""

            ### Asignamos Variables
            _nom_proyec    = str(infoSnip['NOMBRE_INVERSION'])
            _u_formul      = (infoSnip['DES_UNIDAD_UF'])
            _pliego      = str(infoSnip['ENTIDAD'])
            _sector        = infoSnip['FUNCION']
            _m_laudo       = infoSnip['MTO_LAUDO'] 
            _m_fianza       = infoSnip['MTO_CartFza']
            _m_pip         = infoSnip['COSTO_ACTUALIZADO'] + _m_fianza + _m_laudo
            _m_viab        = infoSnip['MTO_VIABLE']
            _m_exptec      = infoSnip['MTO_F15']
            if str(infoSnip['MTO_F16']):
                _m_exptec  = infoSnip['MTO_F16']
            _est_pry       = str(infoSnip['SITUACION'])
            _progr         = str(infoSnip['DES_PROGRAMA'])
            _sub_progr     = str(infoSnip['DES_SUB_PROGRAMA'])
            _beneficiarios = str(infoSnip['BENEFICIARIO'])
            if ( str(infoSnip['COD_SNIP'])).replace('.0','') == _cod_unif:
                _cod_snip  = 'SIN COD.'
            else:
                _cod_snip  = str(infoSnip['COD_SNIP']).replace('.0','')
            # _marcas        = str(infoSnip['Marcas'])
            _marco         = str(infoSnip['MARCO'])
            # _por_etapas    = str(infoSnip['FlagEtapas']) #FlagEtapas
            _informe_cierre= str(infoSnip['CIERRE_REGISTRADO']) #FlagCerrado
            _expediente_tecnico= str(infoSnip['ET_REGISTRADO']) #ET
            _ueiJSON           = str(infoSnip['DES_UNIDAD_UEI'])
            _uei               = data['ger_direc']
            _tipo_proyectoJSON     = str(infoSnip['TIPO_FORMATO'])
            _tipo_proyecto     = data['tipo_inversion']
            _tipo_emergencia   = data['tipo_emergencia']
            _tipo_pry = data['tipo_pry']
            _cierre = str(infoSnip['CIERRE_REGISTRADO'])
            _opmi = str(infoSnip['NOMBRE_OPMI'])
            _fecha_registro_str = (infoSnip['FEC_REGISTRO'])  
            _formato12b = str(infoSnip['TIENE_F12B']) 
            _estado = str(infoSnip['ESTADO']) 
            _a_exptec = infoSnip['FECHA_ET']
            if infoSnip['ID_PROYECTO'] == 0:
                _id_proyecto = str(infoSnip['COD_SNIP']) + "/" + str(infoSnip['COD_TIPO_INVERSION']) + "/0"
            else:
                _id_proyecto = infoSnip['ID_PROYECTO']

            if  _a_exptec != None :
                    _a_exptec = _a_exptec.split('(')
                    _a_exptec = _a_exptec[1].split(')')
                    _a_exptec = int(_a_exptec[0])/1000
                    _a_exptec = datetime.fromtimestamp(_a_exptec).strftime('%Y-%m-%d')   

            if  _fecha_registro_str != None :
                    _fecha_registro_str = _fecha_registro_str.split('(')
                    _fecha_registro_str = _fecha_registro_str[1].split(')')
                    _fecha_registro_str = int(_fecha_registro_str[0])/1000
                    _fecha_registro_str = datetime.fromtimestamp(_fecha_registro_str).strftime('%Y-%m-%d') 

            _tipo_emergencia = str(infoSnip['TIP_EMERGENCIA']) 
                
            if _ueiJSON is not None:
                    if _ueiJSON.find('INFRAESTRUCTURA') >= 0:
                        _uei = 'GERENCIA REGIONAL DE INFRAESTRUCTURA'
                    elif _ueiJSON.find('TRANSPORTES Y COMUNICACIONES') >= 0:
                        _uei = 'DIRECCION REGIONAL DE TRANSPORTE Y COMUNICACIONES'
                    elif _ueiJSON.find('RECURSOS NATURALES') >= 0:
                        _uei = 'GERENCIA REGIONAL DE RECURSOS NATURALES Y GESTION DEL MEDIO AMBIENTE'
                    elif _ueiJSON.find('ECONOMICO') >= 0:
                        _uei = 'GERENCIA REGIONAL DE DESARROLLO ECONOMICO'
                    elif _ueiJSON.find('SOCIAL') >= 0:
                        _uei = 'GERENCIA REGIONAL DE DESARROLLO SOCIAL'
                    elif _ueiJSON.find('AGRICULTURA') >= 0:
                        _uei =  'DIRECCION REGIONAL DE AGRICULTURA'
                    elif _ueiJSON.find('LIMA SUR') >= 0:
                        _uei = 'GERENCIA SUB REGIONAL LIMA SUR'

            if _tipo_proyectoJSON is not None and dateExists(_cod_unif) == True:
                if _tipo_proyectoJSON == 'PROYECTO DE INVERSION':
                    _tipo_proyecto = 'PIP'
                    _tipo_pry = 'PIP'
                elif _tipo_proyectoJSON == 'IOARR' :
                    _tipo_proyecto = 'IOARR'
                    _tipo_pry = 'NO PIP'
                elif _tipo_proyectoJSON == 'FUR':
                    _tipo_proyecto = 'RCC'
                    _tipo_pry = 'RCC'
                elif _tipo_proyectoJSON == 'PROYECTO':
                    if _estado == 'NO REGISTRADO EN EL INVIERTE' and _uei == 'GERENCIA REGIONAL DE DESARROLLO ECONOMICO' and _cod_unif not in ('2016766','2089754','2001621','2000270','2001707') :
                        _tipo_proyecto = 'PROCOMPITE'
                        _tipo_pry = 'PROCOMPITE'
                    else:
                        _tipo_proyecto = 'PIP'
                        _tipo_pry = 'PIP'

                
            # if _opmi is None:
            #     data = requester.requestverCarteradeInversiones(_cod_unif)
            #     if data:
            #         for items in data:
            #             _opmi = items["NOMBRE_ENTIDAD"]

            # if _cierre == "":
            #     mef = requester.requestMEF(_cod_unif)
            #     if mef:
            #         formato12b = mef[0]  
            #         _cierre = formato12b['CIERRE_REGISTRADO']
            _exptec_pdf = str(data['exptec_pdf'])
            _f_exptec = str(data['f_exptec'])

            mydata =  requester.requestFormato08(codigoUnif)

            if len(_exptec_pdf) > 0:
                expediente_tecnico = requester.data_et(codigoUnif,mydata)
                _exptec_pdf = str(expediente_tecnico[0]['et'])
                if _exptec_pdf == "SIN ET" :
                    _f_exptec = None
                else:
                    _f_exptec = str(expediente_tecnico[0]['fecha'])

            _expediente_tecnico_C = requester.data_et_c(codigoUnif,mydata)
            _etapa_f8 = requester.etapa_f8(mydata)
            # Formato 12-b
            mef = requester.requestMEF(codigoUnif)
            _avance_fisico    = 0.00
            if mef:
                formato12b = mef[0]   
                _avance_fisico    = formato12b['PORC_AVANCE_EJEC']
                if _avance_fisico > 100 :
                    _avance_fisico = _avance_fisico/100
            etapa = None
            if  _cod_unif != '2001621' and _cod_unif != '2016766':
                etapa = etapa_inversion(_cod_unif,infoSnip['SITUACION'],infoSnip['DEV_ACUMULADO'],infoSnip['COSTO_ACTUALIZADO'],infoSnip['CIERRE_REGISTRADO'],_etapa_f8,mef)

            _mes_ano_pri_dev= infoSnip['MES_ANO_PRI_DEV']
            _mes_ano_ult_dev= infoSnip['MES_ANO_ULT_DEV']

            print (' _______________________________________________________________')
            print ('|_IDProyecto -> ' + str(idProyecto))
            print ('|_Proyecto -> ' + str(_nom_proyec))
            print ('|_UEI -> ' + str(_u_formul))
            print ('|_Unif -> ' + str(_cod_unif))
            print ('|_Snip -> ' + str(_cod_snip))
            print ('|_Sector -> ' + str(_sector))
            print ('|_Monto PIP -> ' + str(_m_pip))
            print ('|_Monto Viable -> ' + str(_m_viab))
            print ('|_Monto Expediente Tecnico -> ' + str(_m_exptec))
            print ('|_Estado del proyecto -> ' + str(_est_pry))
            print ('|_Programa -> ' + str(_progr))
            print ('|_Sub-Programa -> ' + str(_sub_progr))
            print ('|_Monto Expediente Tecnico Reformulado -> ' + str(_m_exptec))
            print ('|_¿Por Etapas? -> ' + str(_poretapas))
            print ('|_Beneficiarios -> ' + str(_beneficiarios))
            print ('|_Marcas -> ' + str(_marcas))
            print ('|_Marco -> ' + str(_marco))
            print ('|_Por Etapas -> ' + str(_por_etapas))
            print ('|_Informe de Cierre? -> ' + str(_informe_cierre))
            print ('|_Etapa -> ' + str(etapa))
            print ('|_______________________________________________________________')

            con.getCur().execute("""
                UPDATE grli_pip_total_priori SET
                    nom_proyec = %s,
                    cod_unif   = %s,
                    cod_snip   = %s,
                    sector     = %s,
                    m_pip      = %s,
                    m_viab     = %s,
                    m_exptec   = %s,
                    est_pry    = %s,
                    progr      = %s,
                    sub_progr  = %s,
                    marcas  = %s,
                    marco = %s,
                    formato12b = %s,
                    informe_cierre = %s,
                    expediente_tecnico_registrado = %s,
                    ger_direc = %s,
                    tipo_inversion = %s,
                    tipo_pry = %s,
                    cierre = %s,
                    opmi = %s,
                    tipo_emergencia = %s,
                    fecha_registro_str = %s,
                    pliego = %s,
                    estado = %s,
                    a_exptec = %s,
                    exptec_pdf = %s,
                    f_exptec = %s,
                    beneficiarios = %s,
                    expediente_c = %s,
                    id_proyecto = %s,
                    etapa_f8 = %s,
                    a_fisico = %s,
                    monto_laudo = %s,
                    monto_fianza = %s,
                    etapa = %s,
                    mes_ano_pri_dev = %s,
                    mes_ano_ult_dev = %s
                WHERE id = %s
            """, (
                    _nom_proyec,
                    _cod_unif,
                    _cod_snip,
                    _sector,
                    _m_pip,
                    _m_viab,
                    _m_exptec,
                    _est_pry,
                    _progr,
                    _sub_progr,
                    _marcas,
                    _marco,
                    _formato12b,
                    _informe_cierre,
                    _expediente_tecnico,
                    _uei,
                    _tipo_proyecto,
                    _tipo_pry,
                    _cierre,
                    _opmi,
                    _tipo_emergencia,
                    _fecha_registro_str,
                    _pliego,
                    _estado,
                    _a_exptec,
                    _exptec_pdf,
                    _f_exptec,
                    _beneficiarios,
                    _expediente_tecnico_C,
                    _id_proyecto,
                    _etapa_f8,
                    _avance_fisico,
                    _m_laudo,
                    _m_fianza,
                    etapa,
                    _mes_ano_pri_dev,
                    _mes_ano_ult_dev,
                    idProyecto
            ))

        # con.getConn().commit()
    # Datos Financieros del SSI
    def updateWithInfoSiafFinanciera(infoSiafdata, codigoUnif, idProyecto):
        for infoSiaf in infoSiafdata:
            _m_pim_a_todas_ue    = Decimal(0.00)
            _m_deveng_a_todas_ue = Decimal(0.00)
            _a_financ_a          = Decimal(0.00)
            _tipo_proyecto       = infoSiaf['TIPO_FORMATO']
            ### Info Financiera
            
            fecha = str(datetime.now().date())
            # time.sleep(5)
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

            con.getCur().execute("""
                SELECT MAX(anio_financ) AS anio
                FROM inf_financiera
                WHERE cod_unif = %s
            """, (codigoUnif,) )

            _ult_anio_ejec_pry_financ = con.getCur().fetchone()['anio'] or '0'

            con.getCur().execute("""
                SELECT
                    SUM(pim::numeric) AS pim_acu,
                    SUM(dev::numeric) AS dev_acu,
                    grli_pip_total_priori.m_pip
                FROM inf_financiera
                INNER JOIN grli_pip_total_priori ON grli_pip_total_priori.cod_unif = inf_financiera.cod_unif::text
                WHERE   inf_financiera.cod_unif = %s
                GROUP BY grli_pip_total_priori.m_pip
            """, (codigoUnif,) )

            acumulado = con.getCur().fetchone()

            _m_pip   = 0.00
            _pim_acu = 0.00
            _dev_acu = 0.00

            if acumulado:
                _m_pip   = acumulado['m_pip']
                _pim_acu = acumulado['pim_acu']
                _dev_acu = acumulado['dev_acu']


            con.getCur().execute("""
                SELECT
                    SUM(pim::numeric) AS pim,
                    SUM(dev::numeric) AS dev
                FROM inf_financiera
                WHERE cod_unif = %s AND anio_financ = %s
            """, (codigoUnif,_ult_anio_ejec_pry_financ) )

            ultimo = con.getCur().fetchone()

            _pim = 0.00
            _dev = 0.00

            if ultimo:
                _pim = ultimo['pim'] or 0.00
                _dev = ultimo['dev'] or 0.00

            try:
                _a_financ = str(twoDec(Decimal(str(_dev)) / Decimal(str(_pim)) * 100))
            except:
                _a_financ = Decimal(0.00)

            try:
                _a_financ_a = str(twoDec(Decimal(str(_m_deveng_a_todas_ue)) / Decimal(str(_m_pip)) * 100))
            except:
                _a_financ_a = Decimal(0.00)


            print (' ________________________________________________________')
            print ('|_ Ultimo año ejecución financiera -> ' + str(_ult_anio_ejec_pry_financ))

            print ('|_ Devengado ultimo Año-> ' + str(_dev))
            print ('|_ Pim Ultimo Año -> ' + str(_pim))

            print ('|_ Devengado acumulado-> ' + str(_dev_acu))
            print ('|_ Pim acumulado -> ' + str(_pim_acu))

            print ('|_ Devengado acumulado Total -> ' + str(_m_deveng_a_todas_ue))
            print ('|_ Pim acumulado Total -> ' + str(_m_pim_a_todas_ue))
            print ('|_ Avance Anual -> ' + str(_a_financ))
            print ('|_ Avance Total -> ' + str(_a_financ_a))
            print (' ________________________________________________________')

            con.getCur().execute("""
                UPDATE grli_pip_total_priori SET
                    ult_anio_ejec_pry_financ = %s,
                    m_pim                    = %s,
                    m_pim_acu                = %s,
                    m_deveng                 = %s,
                    m_deveng_a               = %s,
                    m_deveng_a_todas_ue      = %s,
                    m_pim_a_todas_ue         = %s,
                    a_financ                 = %s,
                    a_financ_a               = %s,
                    f_deveng_a               = %s,
                    tipo_proyecto            = %s,
                    anio                     = %s,
                    fecha                    = %s
                WHERE  id = %s
            """,(
                    str(_ult_anio_ejec_pry_financ),
                    str(_pim),
                    str(_pim_acu),
                    str(_dev),
                    str(_dev_acu),
                    str(_m_deveng_a_todas_ue),
                    str(_m_pim_a_todas_ue),
                    str(_a_financ),
                    str(_a_financ_a),
                    str(datetime.now().date()),
                    str(_tipo_proyecto),
                    str(_ult_anio_ejec_pry_financ),
                    datetime.now().date(),
                    idProyecto
            ))

            con.getConn().commit()

    def updateWithInfoSiafContratos(infContrato, codigoUnif,idProyecto):
        # con.getCur().execute("""
        #     DELETE FROM grli_pip_total_priori_contratos
        #     WHERE idproyecto = %s
        # """, ( str(idProyecto), ) )
        for contrato in infContrato:
            print (' ________________________________________________________________')
            print ('|_ Ejecutora -> ' + str('N/A'))
            print ('|_ Tipo Proceso -> ' + str(contrato['OBJETO_CONTRATAC']))
            # print ('|_ Contratista RUC -> ' + str(contrato['OBJETO_CONTRATAC']))
            print ('|_ Contratista Nombre -> ' + str(contrato['NOM_CONTRATISTA']))
            print ('|_ Número de Contrato ->' + str(contrato['FEC_SUSCRIPCION']))
            # print ('|_ Fecha de Contrato ->' + str(contrato['ContratoFecha']))
            # print ('|_ Número de Contrato ->' + str(contrato['ContratoNumero']))
            # print ('|_ Fecha de Contrato Str ->' + str(contrato['ContratoFechaStr']))
            print ('|_ Descripción del Contrato ->' + str(contrato['DES_PROCESO']))
            # print ('|_ Contrato Moneda ->' + str(contrato['ContratoMoneda']))
            print ('|_ Contrato Monto ->' + str(contrato['MTO_ITEM']))
            print ('|________________________________________________________________')

            con.getCur().execute("""
                INSERT INTO grli_pip_total_priori_contratos_ssi(
                    codigo_unico,
                    contrato_nro,
                    ejecutora,
                    contratista,
                    contrato_moneda,
                    contratista_ruc,
                    tipo_proceso,
                    descripcion,
                    contrato_monto,
                    contrato_fecha,
                    fecha
                )
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
                (
                    str(codigoUnif),
                    str(contrato['NUM_CONTRATO']),
                    str('REGION LIMA'),
                    str(contrato['NOM_CONTRATISTA']),
                    str('S/'),
                    str('N/A'),
                    str(contrato['OBJETO_CONTRATAC']),
                    str(contrato['DES_PROCESO']),
                    str(contrato['MTO_ITEM']),
                    str(contrato['FEC_SUSCRIPCION']),
                    str(str(datetime.now().date()))
            ))

            # con.getConn().commit()

    def __init__():

        #beginID = raw_input('¿Desde que ID quiere comenzar la actualización? [1]') or "1"
        # Query the database and obtain data as Python objects
        projects = getProjects()

        for i in projects:

            print ('********************************************************************************************* CODIGO SIAF <<' + i['cod_unif'] + '>> AÑO ' + str(years) + ' PARA TABLA INF_FINANCIERA(PROYECTOS) '  + str(opc) + '**********************************************************************************************')
            print ('ID -> ' + str(i['id']))
            # VARS
            codigoSnip, codigoUnif, idProyecto, flagSnip = i['cod_snip'], i['cod_unif'], i['id'], 'false'

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
            codigo = codigoUnif
            #Si No tiene codigo UNIFICADO, se buscará Usando el Codigo SNIP
            if codigo == 'SIN COD.':
                codigo = codigoSnip
                flagSnip = 'true'
                print ("EL PROYECTO NO POSEE CODIGO UNIFICADO EN LA BASE DE DATOS, SE BUSCARA CON CODIGO SNIP")


            infoSnip = ""
            infoSiaf = ""
            # infContrato = ""
            ########################################################################################################################
            #################################################### INFO SNIP #########################################################
            ########################################################################################################################

            infoSiaf = requester.requestInfoSnip(codigo,'SIAF',20)

            # if infoSnip:
            #     updateWithInfoSnip(infoSnip, codigoSnip, codigoUnif, idProyecto,i)
            # else:
            #     print ("NO SE ENCONTRARON DATOS DE INFOSNIP")

            ########################################################################################################################
            #################################################### INFO SIAF #########################################################
            ########################################################################################################################

            # infoSiaf = requester.requestInfoSiaf(codigo,flagSnip)
            # infContrato = requester.requestContrato(codigoSnip)
            
            # if len(infContrato) == 0 :
            #     infContrato = requester.requestContrato(codigoSnip)

            # print(infContrato)
            
            if infoSiaf:
                updateWithInfoSnip(infoSiaf, codigoSnip, codigoUnif, idProyecto,i)
                updateWithInfoSiafFinanciera(infoSiaf, codigoUnif, idProyecto)
                # print((infContrato))
                
                # con.getConn().commit()
                # updateWithInfoSiafContratos(infContrato,codigoUnif, idProyecto)
                # con.getConn().commit()
            else:
                print ("NO SE ENCONTRARON DATOS DE INFOSIAF")
        
    __init__()
