# -*- coding: utf-8 -*-
import sys
import requester
from decimal import Decimal
from commons import twoDec
from importlib import reload
import datetime

reload(sys)  # UTF 8
#sys.setdefaultencoding('UTF8') Para Python 2

def main(con,beginID,endID):

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
        con.getCur().execute("""
            SELECT
                cod_snip,
                cod_unif,
                id as id
            FROM grli_pip_total_priori
            WHERE id >= %s AND id <= %s AND
            (cod_unif != 'SIN COD.' or cod_snip != 'SIN COD.')
            ORDER BY id
        """, ( str(beginID),str(endID) ) )

        return con.getCur().fetchall()

    def updateWithInfoSnip(infoSnip, codigoSnip, codigoUnif, idProyecto):
        _nom_proyec       =   ""
        _cod_snip         =   codigoSnip
        _cod_unif         =   codigoUnif
        _u_formul         =   ""
        _sector           =   ""
        _m_pip            =   Decimal(0.00)
        _m_viab           =   Decimal(0.00)
        _m_exptec         =   Decimal(0.00)
        _est_pry       =   ""
        _progr            =   ""
        _subprogr         =   ""
        _poretapas        =   0
        _beneficiarios    =   ""
        _ult_anio_ejec_pry_financ = 0
        _marcas           = ""

        ### Asignamos Variables
        _nom_proyec    = str(infoSnip['Nombre'])
        _u_formul      = (infoSnip['Uf'] + ' - ' + infoSnip['EvaluadoraPliego'])
        _sector        = infoSnip['Funcion']
        _m_pip         = infoSnip['Costo']
        _m_viab        = infoSnip['MontoAlternativa']
        _m_exptec      = infoSnip['MontoF15']
        if str(infoSnip['MontoF16']):
            _m_exptec  = infoSnip['MontoF16']
        _est_pry       = str(infoSnip['Situacion'])
        _progr         = str(infoSnip['Programa'])
        _sub_progr      = str(infoSnip['Subprograma'])
        #_poretapas     = str(infoSnip['FlagEtapas'])
        _beneficiarios = str(infoSnip['Beneficiario'])
        _cod_snip      = str(infoSnip['CodigoSnip']).replace(".0","")
        _marcas        = str(infoSnip['Marcas'])

        print (' _______________________________________________________________')
        print ('|_IDProyecto -> ' + str(idProyecto))
        print ('|_Proyecto -> ' + str(_nom_proyec))
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
                marcas  = %s
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
                idProyecto
        ))

        con.getConn().commit()

    def updateWithInfoSiafFinanciera(infoSiaf, codigoUnif, idProyecto):

        _m_pim_a_todas_ue    = Decimal(0.00)
        _m_deveng_a_todas_ue = Decimal(0.00)
        _a_financ_a          = Decimal(0.00)
        ### Info Financiera

        con.getCur().execute(""" DELETE FROM inf_financiera where cod_unif = """ + str(codigoUnif))
        con.getConn().commit()

        for e in infoSiaf['InfoFinanciera']:

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

            _m_pim_a_todas_ue += twoDec(e['Pim'])
            _m_deveng_a_todas_ue += twoDec(e['Dev'])

            if int(e['CodigoEjecutora']) in unidadesEjecutoras:
                #### Fuente de Financieamiento
                if len(infoSiaf['FuenteFinan']) > 0:
                    for f in infoSiaf['FuenteFinan']:
                        if(f[b'Año'.decode('UTF-8')]) == e[b'Año'.decode('UTF-8')]:
                            _fuente_financ = str(f['Nombre'])
                            break
                else:
                    print 'No hay Fuentes de financiamiento'

                _uni_ejec      = unidadesEjecutoras[int(e['CodigoEjecutora'])]
                _anio_financ   = e[b'Año'.decode('UTF-8')]
                _pia           = str(e['Pia'])
                _pim           = str(e['Pim'])
                _dev           = str(e['Dev'])
                _comp_anual    = str(e['CompromisoAnual'])
                _certif        = str(e['CertificadoAnual'])
                _dev_mar       = e['DevMar']
                _dev_ene       = e['DevEne']
                _dev_feb       = e['DevFeb']
                _dev_abr       = e['DevAbr']
                _dev_may       = e['DevMay']
                _dev_jun       = e['DevJun']
                _dev_jul       = e['DevJul']
                _dev_ago       = e['DevAgo']
                _dev_set       = e['DevSet']
                _dev_oct       = e['DevOct']
                _dev_nov       = e['DevNov']
                _dev_dic       = e['DevDic']

                print ' ________________________________________________________'
                print '|UNIDAD EJECUTORA : '         + str(_uni_ejec)
                print '|Año :              '         + str(_anio_financ)
                print '|_Codigo Ejecutora       -> ' + e['CodigoEjecutora']
                print '|_Fuente Financiamiento  -> ' + _fuente_financ
                print '|_Pia                    -> ' + str(_pia)
                print '|_Pim                    -> ' + str(_pim)
                print '|_Dev                    -> ' + str(_dev)
                print '|_Compromiso Anual       -> ' + str(_comp_anual)
                print '|_Certificado            -> ' + str(_certif)
                print '|_Devengado Enero        -> ' + str(_dev_ene)
                print '|_Devengado Febrero      -> ' + str(_dev_feb)
                print '|_Devengado Marzo        -> ' + str(_dev_mar)
                print '|_Devengado Abril        -> ' + str(_dev_abr)
                print '|_Devengado Mayo         -> ' + str(_dev_may)
                print '|_Devengado Junio        -> ' + str(_dev_jun)
                print '|_Devengado Julio        -> ' + str(_dev_jul)
                print '|_Devengado Agosto       -> ' + str(_dev_ago)
                print '|_Devengado Septiembre   -> ' + str(_dev_set)
                print '|_Devengado Octubre      -> ' + str(_dev_oct)
                print '|_Devengado Noviembre    -> ' + str(_dev_nov)
                print '|_Devengado Diciembre    -> ' + str(_dev_dic)
                print '|________________________________________________________'

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
                            certif) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                        (
                        codigoUnif,_uni_ejec,_anio_financ,_fuente_financ,_pia,_dev_ene,
                        _dev_feb,_dev_mar,_dev_abr,_dev_may,_dev_jun,_dev_jul,
                        _dev_ago,_dev_set,_dev_oct,_dev_nov,_dev_dic,_dev,_pim,
                        _comp_anual,_certif
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
            FROM inf_financiera INNER JOIN grli_pip_total_priori ON grli_pip_total_priori.cod_unif = inf_financiera.cod_unif ::text
            WHERE inf_financiera.cod_unif = %s
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
            SELECT SUM(pim::numeric) AS pim, SUM(dev::numeric) AS dev
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


        print ' ________________________________________________________'
        print '|_ Ultimo año ejecución financiera -> ' + str(_ult_anio_ejec_pry_financ)

        print '|_ Devengado ultimo Año-> ' + str(_dev)
        print '|_ Pim Ultimo Año -> ' + str(_pim)

        print '|_ Devengado acumulado-> ' + str(_dev_acu)
        print '|_ Pim acumulado -> ' + str(_pim_acu)

        print '|_ Devengado acumulado Total -> ' + str(_m_deveng_a_todas_ue)
        print '|_ Pim acumulado Total -> ' + str(_m_pim_a_todas_ue)
        print '|_ Avance Anual -> ' + str(_a_financ)
        print '|_ Avance Total -> ' + str(_a_financ_a)
        print ' ________________________________________________________'

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
                f_deveng_a               = %s
            WHERE id = %s
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
                str(datetime.datetime.now().date()),
                idProyecto
        ))

        con.getConn().commit()

    def updateWithInfoSiafContratos(infoSiaf, idProyecto):
        con.getCur().execute("""
                DELETE FROM grli_pip_total_priori_contratos
                WHERE idproyecto = %s
            """, ( str(idProyecto), ) )
        for contrato in infoSiaf['Contratos']:
            #print ' ________________________________________________________________'
            #print '|_ Ejecutora -> ' + str(contrato['Ejecutora'])
            #print '|_ Tipo Proceso -> ' + str(contrato['TipoProceso'])
            #print '|_ Contratista RUC -> ' + str(contrato['ContratistaRUC'])
            #print '|_ Contratista Nombre -> ' + str(contrato['ContratistaNombre'])
            #print '|_ Número de Contrato ->' + str(contrato['ContratoNumero'])
            #print '|_ Fecha de Contrato ->' + str(contrato['ContratoFecha'])
            #print '|_ Número de Contrato ->' + str(contrato['ContratoNumero'])
            #print '|_ Fecha de Contrato Str ->' + str(contrato['ContratoFechaStr'])
            #print '|_ Descripción del Contrato ->' + str(contrato['ContratoDescripcion'])
            #print '|_ Contrato Moneda ->' + str(contrato['ContratoMoneda'])
            #print '|_ Contrato Monto ->' + str(contrato['ContratoMonto'])
            #print '|________________________________________________________________'

            con.getCur().execute("""
                INSERT INTO grli_pip_total_priori_contratos(
                    idproyecto,
                    contrato_nro,
                    ejecutora,
                    contratista,
                    contrato_moneda,
                    contratista_ruc,
                    tipo_proceso,
                    descripcion,
                    contrato_monto,
                    contrato_fecha
                )
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
                (
                    str(idProyecto),
                    str(contrato['ContratoNumero']),
                    str(contrato['Ejecutora']),
                    str(contrato['ContratistaNombre']),
                    str(contrato['ContratoMoneda']),
                    str(contrato['ContratistaRUC']),
                    str(contrato['TipoProceso']),
                    str(contrato['ContratoDescripcion']),
                    str(contrato['ContratoMonto']),
                    str(contrato['ContratoFechaStr'])
            ))

            con.getConn().commit()

    def __init__():

        #beginID = raw_input('¿Desde que ID quiere comenzar la actualización? [1]') or "1"
        # Query the database and obtain data as Python objects
        projects = getProjects()

        for i in projects:

            print ('********************************************************************************************* CODIGO SIAF <<' + i['cod_unif'] + '>> **********************************************************************************************')
            print (' ID -> ' + str(i['id']))
            # VARS
            codigoSnip, codigoUnif, idProyecto, flagSnip = i['cod_snip'], i['cod_unif'], i['id'], 'false'

            print 'Iniciando ...'

            # VARS TO INSERT
            print 'Inicializando Variables...'

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
                print "El proyecto no posee codigo unificado en la base de datos, Se Buscará Con Código SNIP"


            infoSnip = ""
            infoSiaf = ""
            ########################################################################################################################
            #################################################### INFO SNIP #########################################################
            ########################################################################################################################

            infoSnip = requester.requestInfoSnip(codigo,flagSnip)

            if infoSnip:
                updateWithInfoSnip(infoSnip, codigoSnip, codigoUnif, idProyecto)
            else:
                print ('No se Encontraron Datos en InfoSnip')

            ########################################################################################################################
            #################################################### INFO SIAF #########################################################
            ########################################################################################################################

            infoSiaf = requester.requestInfoSiaf(codigo,flagSnip)

            if infoSiaf:
                updateWithInfoSiafFinanciera(infoSiaf, codigoUnif, idProyecto)
                if infoSiaf['Contratos']:
                    updateWithInfoSiafContratos(infoSiaf, idProyecto)
            else:
                print "No se Encontraron Datos en InfoSiaf"

    __init__()
