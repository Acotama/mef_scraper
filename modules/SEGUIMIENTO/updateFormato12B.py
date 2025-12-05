import os
import json
import requester
import reader
from datetime import datetime, timedelta
from decimal import Decimal

# from colorama import Fore

def main(con,years):

    def getformato12b():
        con.getCur().execute("""  select distinct cod_unif from vw_lista_formato1b_extraer """)
        return  con.getCur().fetchall()
    
    def lista_formato_12b(fecha):
        con.getCur().execute("""select distinct cod_unif from formato12b where fecha_subida = %s """ , (fecha,))
        resultados =   con.getCur().fetchall()
        resultados_lista = [row['cod_unif'] for row in resultados]
        return resultados_lista
        # con.getConn().commit()

    def insertFormato12b(data,fecha):
        lista_f12b =  lista_formato_12b(fecha)
        for row in data:
            codigo = row['cod_unif']
            if int(codigo) not in  lista_f12b:
                print("CONSULTANDO FORMATO 12-B")

                # #Nombre Proyecto
                # nombreProyecto = row.contents[0].text
                # #Codigo Unificado del proyecto
                # codigo = nombreProyecto.split(':')[0].strip()
                print ('FORMATO ['+str(codigo)+'] -> Tabla [formato12b]...')
                mef = requester.requestMEF(codigo)
                
                if mef:
                    formato12b = mef[0]   
                    #### Datos Formato 12B
                    _fech_act         = None
                    _exp_reg          = "NO" 
                    _pro_ene          = 0.00
                    _pro_feb          = 0.00
                    _pro_mar          = 0.00
                    _pro_abr          = 0.00
                    _pro_may          = 0.00
                    _pro_jun          = 0.00
                    _pro_jul          = 0.00
                    _pro_ago          = 0.00
                    _pro_set          = 0.00
                    _pro_oct          = 0.00
                    _pro_nov          = 0.00
                    _pro_dic          = 0.00
                    _act_ene          = 0.00
                    _act_feb          = 0.00
                    _act_mar          = 0.00
                    _act_abr          = 0.00
                    _act_may          = 0.00
                    _act_jun          = 0.00
                    _act_jul          = 0.00
                    _act_ago          = 0.00
                    _act_set          = 0.00
                    _act_oct          = 0.00
                    _act_nov          = 0.00
                    _act_dic          = 0.00
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
                    _total_pro        = 0.00
                    _total_act        = 0.00
                    _total_dev        = 0.00
                    _fecha_et         = None
                    _tipo_formato     = ""
                    _modal_ejec       = ""
                    _avance_fisico    = 0.00
                    _ult_est_situal   = None
                    _ult_periodo_reg_f12b =""
                    _fec_ini_ejec    = None
                    _fec_fin_ejec    = None
                    _avance_ejecucion= 0.00
                    _cierre          = ""
                    _fec_declara_estim    = None
                    _costo_actualizado = 0.00
                    _devengado_acumulado =  0.00

                    #LLENANDO DATOS
                    _fech_act         = formato12b['FECHA_ULT_ACT_F12B']
                    _pro_ene          = formato12b['MONTO_PROGRAMADO_1']
                    _pro_feb          = formato12b['MONTO_PROGRAMADO_2']
                    _pro_mar          = formato12b['MONTO_PROGRAMADO_3']
                    _pro_abr          = formato12b['MONTO_PROGRAMADO_4']
                    _pro_may          = formato12b['MONTO_PROGRAMADO_5']
                    _pro_jun          = formato12b['MONTO_PROGRAMADO_6']
                    _pro_jul          = formato12b['MONTO_PROGRAMADO_7']
                    _pro_ago          = formato12b['MONTO_PROGRAMADO_8']
                    _pro_set          = formato12b['MONTO_PROGRAMADO_9']
                    _pro_oct          = formato12b['MONTO_PROGRAMADO_10']
                    _pro_nov          = formato12b['MONTO_PROGRAMADO_11']
                    _pro_dic          = formato12b['MONTO_PROGRAMADO_12']
                    _act_ene          = formato12b['MONTO_ACTUALIZADO_1']
                    _act_feb          = formato12b['MONTO_ACTUALIZADO_2']
                    _act_mar          = formato12b['MONTO_ACTUALIZADO_3']
                    _act_abr          = formato12b['MONTO_ACTUALIZADO_4']
                    _act_may          = formato12b['MONTO_ACTUALIZADO_5']
                    _act_jun          = formato12b['MONTO_ACTUALIZADO_6']
                    _act_jul          = formato12b['MONTO_ACTUALIZADO_7']
                    _act_ago          = formato12b['MONTO_ACTUALIZADO_8']
                    _act_set          = formato12b['MONTO_ACTUALIZADO_9']
                    _act_oct          = formato12b['MONTO_ACTUALIZADO_10']
                    _act_nov          = formato12b['MONTO_ACTUALIZADO_11']
                    _act_dic          = formato12b['MONTO_ACTUALIZADO_12']
                    _dev_ene          = formato12b['DEV_ANO_VIG_1']
                    _dev_feb          = formato12b['DEV_ANO_VIG_2']
                    _dev_mar          = formato12b['DEV_ANO_VIG_3']
                    _dev_abr          = formato12b['DEV_ANO_VIG_4']
                    _dev_may          = formato12b['DEV_ANO_VIG_5']
                    _dev_jun          = formato12b['DEV_ANO_VIG_6']
                    _dev_jul          = formato12b['DEV_ANO_VIG_7']
                    _dev_ago          = formato12b['DEV_ANO_VIG_8']
                    _dev_set          = formato12b['DEV_ANO_VIG_9']
                    _dev_oct          = formato12b['DEV_ANO_VIG_10']
                    _dev_nov          = formato12b['DEV_ANO_VIG_11']
                    _dev_dic          = formato12b['DEV_ANO_VIG_12']
                    _fecha_et         = formato12b['FECHA_ET']
                    _tipo_formato     = formato12b['TIPO_FORMATO']
                    _modal_ejec       = formato12b['MODAL_EJEC']
                    _avance_fisico    = formato12b['PORC_AVANCE_EJEC']
                    _ult_est_situal   = formato12b['ULT_ESTADO_SITUACIONAL'].strip() if formato12b['ULT_ESTADO_SITUACIONAL'].strip() != '' else None
                    _ult_periodo_reg_f12b = formato12b['ULT_PERIODO_REG_F12B']
                    _fec_ini_ejec = formato12b['FEC_INI_EJ']
                    _fec_fin_ejec = formato12b['FEC_FIN_EJ']
                    _avance_ejecucion = formato12b['PORC_AVANCE_FIS']
                    _cierre           = formato12b['CIERRE_REGISTRADO']
                    _fec_declara_estim    = formato12b['FEC_DECLARA_ESTIM']
                    _costo_actualizado    = formato12b['COSTO_ACTUALIZADO']
                    _devengado_acumulado    = formato12b['DEV_ACUMULADO']
                                                        
                    _total_pro  =  _pro_ene + _pro_feb + _pro_mar + _pro_abr + _pro_may + _pro_jun + _pro_jul + _pro_ago + _pro_set + _pro_oct + _pro_nov + _pro_dic
                    _total_act  =  _act_ene + _act_feb + _act_mar + _act_abr + _act_may + _act_jun + _act_jul + _act_ago + _act_set + _act_oct + _act_nov + _act_dic
                    _total_dev  =  _dev_ene + _dev_feb + _dev_mar + _dev_abr + _dev_may + _dev_jun + _dev_jul + _dev_ago + _dev_set + _dev_oct + _dev_nov + _dev_dic

                    if  _fech_act != None :
                        _fech_act = _fech_act.split('(')
                        _fech_act = _fech_act[1].split(')')
                        _fech_act = int(_fech_act[0])/1000
                        _fech_act = datetime.fromtimestamp(_fech_act).strftime('%Y-%m-%d')   

                    if  _fecha_et != None :
                        _fecha_et = _fecha_et.split('(')
                        _fecha_et = _fecha_et[1].split(')')
                        _fecha_et = int(_fecha_et[0])/1000
                        _fecha_et = datetime.fromtimestamp(_fecha_et).strftime('%Y-%m-%d')             

                    if _total_pro != 0 or _total_act != 0:
                        _exp_reg = "SI"

                    if _tipo_formato == "IOARR":
                        _tipo_formato = requester.requestIOARREmergencia(codigo)

                    #Para grli_pip_total_priori
                    if _tipo_formato is not None:
                        if _tipo_formato == 'PROYECTO DE INVERSION':
                            _tipo_proyecto = 'PIP'
                            _tipo_pry = 'PIP'
                        elif _tipo_formato == 'IOARR':
                            _tipo_proyecto = _tipo_formato
                            _tipo_pry = 'NO PIP'
                        elif _tipo_formato == 'FUR':
                            _tipo_proyecto = 'RCC'
                            _tipo_pry = 'RCC'

                    # con.getCur().execute("""
                    #     UPDATE grli_pip_total_priori SET
                    #         tipo_inversion = %s,
                    #         tipo_pry = %s,
                    #         cierre = %s,
                    #         formato12b = %s
                    #     WHERE cod_unif = %s
                    # """, (
                    #         _tipo_proyecto,
                    #         _tipo_pry,
                    #         _cierre,
                    #         'SI',
                    #         codigo
                    # ))

                    con.getCur().execute("""
                        INSERT INTO formato12b(
                            cod_unif,
                            exp_reg,
                            fecha_actual,
                            year,
                            p_enero,
                            p_febrero,
                            p_marzo,
                            p_abril,
                            p_mayo,
                            p_junio,
                            p_julio,
                            p_agosto,
                            p_setiembre,
                            p_octubre,
                            p_noviembre,
                            p_diciembre,
                            a_enero,
                            a_febrero,
                            a_marzo,
                            a_abril,
                            a_mayo,
                            a_junio,
                            a_julio,
                            a_agosto,
                            a_setiembre,
                            a_octubre,
                            a_noviembre,
                            a_diciembre,
                            fecha_subida,
                            fecha_et,     
                            tipo_formato,
                            modal_ejec,
                            avance_fisico,
                            ult_est_situal,
                            ult_periodo_reg_f12b,
                            fec_ini_ejec,
                            fec_fin_ejec,
                            avance_ejecucion,
                            cierre,
                            dev_enero,
                            dev_febrero,
                            dev_marzo,
                            dev_abril,
                            dev_mayo,
                            dev_junio,
                            dev_julio,
                            dev_agosto,
                            dev_setiembre,
                            dev_octubre,
                            dev_noviembre,
                            dev_diciembre,
                            total_programado,
                            total_actualizado,
                            total_devengado,
                            fec_declara_estim,
                            costo_actualizado,
                            devengado_acumulado
                            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                            (codigo,_exp_reg,_fech_act,years,_pro_ene,_pro_feb,_pro_mar,_pro_abr,_pro_may,_pro_jun,_pro_jul,_pro_ago,_pro_set,_pro_oct,_pro_nov,
                            _pro_dic,_act_ene,_act_feb,_act_mar,_act_abr,_act_may,_act_jun,_act_jul,_act_ago,_act_set,_act_oct,_act_nov,_act_dic,fecha,_fecha_et,     
                            _tipo_formato,_modal_ejec,_avance_fisico,_ult_est_situal,_ult_periodo_reg_f12b,_fec_ini_ejec,_fec_fin_ejec,_avance_ejecucion,_cierre,
                            _dev_ene,_dev_feb,_dev_mar,_dev_abr,_dev_may,_dev_jun,_dev_jul,_dev_ago,_dev_set,_dev_oct,_dev_nov,_dev_dic,_total_pro,_total_act,_total_dev,
                            _fec_declara_estim,_costo_actualizado,_devengado_acumulado
                    ))
                    print ('GUARDANDO FORMATO ['+str(codigo)+'] -> Tabla [formato12b]...')
                else:
                    # con.getCur().execute("""
                    #     UPDATE grli_pip_total_priori SET
                    #         formato12b = %s
                    #     WHERE cod_unif = %s
                    # """, (
                    #         'NO',
                    #         codigo
                    # ))

                    #LLENANDO DATOS
                    _fech_act         = None
                    _pro_ene          = None
                    _pro_feb          = None
                    _pro_mar          = None
                    _pro_abr          = None
                    _pro_may          = None
                    _pro_jun          = None
                    _pro_jul          = None
                    _pro_ago          = None
                    _pro_set          = None
                    _pro_oct          = None
                    _pro_nov          = None
                    _pro_dic          = None
                    _act_ene          = None
                    _act_feb          = None
                    _act_mar          = None
                    _act_abr          = None
                    _act_may          = None
                    _act_jun          = None
                    _act_jul          = None
                    _act_ago          = None
                    _act_set          = None
                    _act_oct          = None
                    _act_nov          = None
                    _act_dic          = None
                    _dev_ene          = None
                    _dev_feb          = None
                    _dev_mar          = None
                    _dev_abr          = None
                    _dev_may          = None
                    _dev_jun          = None
                    _dev_jul          = None
                    _dev_ago          = None
                    _dev_set          = None
                    _dev_oct          = None
                    _dev_nov          = None
                    _dev_dic          = None
                    _fecha_et         = None
                    _tipo_formato     = None #ver
                    _modal_ejec       = None
                    _avance_fisico    = None
                    _ult_est_situal   = None
                    _ult_periodo_reg_f12b = None
                    _fec_ini_ejec = None
                    _fec_fin_ejec = None
                    _avance_ejecucion = None
                    _cierre           = None
                    _fec_declara_estim    = None
                    _costo_actualizado    = None
                    _devengado_acumulado    = None
                                                        
                    _total_pro  =  None
                    _total_act  =  None
                    _total_dev  =  None

                    _fech_act = None
                    _fecha_et = None     
                    _exp_reg = None
                    _tipo_formato = None

                    con.getCur().execute("""
                        INSERT INTO formato12b(
                            cod_unif,
                            exp_reg,
                            fecha_actual,
                            year,
                            p_enero,
                            p_febrero,
                            p_marzo,
                            p_abril,
                            p_mayo,
                            p_junio,
                            p_julio,
                            p_agosto,
                            p_setiembre,
                            p_octubre,
                            p_noviembre,
                            p_diciembre,
                            a_enero,
                            a_febrero,
                            a_marzo,
                            a_abril,
                            a_mayo,
                            a_junio,
                            a_julio,
                            a_agosto,
                            a_setiembre,
                            a_octubre,
                            a_noviembre,
                            a_diciembre,
                            fecha_subida,
                            fecha_et,     
                            tipo_formato,
                            modal_ejec,
                            avance_fisico,
                            ult_est_situal,
                            ult_periodo_reg_f12b,
                            fec_ini_ejec,
                            fec_fin_ejec,
                            avance_ejecucion,
                            cierre,
                            dev_enero,
                            dev_febrero,
                            dev_marzo,
                            dev_abril,
                            dev_mayo,
                            dev_junio,
                            dev_julio,
                            dev_agosto,
                            dev_setiembre,
                            dev_octubre,
                            dev_noviembre,
                            dev_diciembre,
                            total_programado,
                            total_actualizado,
                            total_devengado,
                            fec_declara_estim,
                            costo_actualizado,
                            devengado_acumulado
                            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                            (codigo,_exp_reg,_fech_act,years,_pro_ene,_pro_feb,_pro_mar,_pro_abr,_pro_may,_pro_jun,_pro_jul,_pro_ago,_pro_set,_pro_oct,_pro_nov,
                            _pro_dic,_act_ene,_act_feb,_act_mar,_act_abr,_act_may,_act_jun,_act_jul,_act_ago,_act_set,_act_oct,_act_nov,_act_dic,fecha,_fecha_et,     
                            _tipo_formato,_modal_ejec,_avance_fisico,_ult_est_situal,_ult_periodo_reg_f12b,_fec_ini_ejec,_fec_fin_ejec,_avance_ejecucion,_cierre,
                            _dev_ene,_dev_feb,_dev_mar,_dev_abr,_dev_may,_dev_jun,_dev_jul,_dev_ago,_dev_set,_dev_oct,_dev_nov,_dev_dic,_total_pro,_total_act,_total_dev,
                            _fec_declara_estim,_costo_actualizado,_devengado_acumulado
                    ))
                    print ('GUARDANDO FORMATO ['+str(codigo)+'] DE UN PROYECTO DESACTIVADO O CERRADO -> Tabla [formato12b]...')
                    # print ("NO HAY INFORMACION DEL FORMATO 12-B => "+ str(codigo))
            else:
                print("YA EXISTE EN LA BASE DE DATOS")
            con.getConn().commit()


    def __init__():
 
        fechahoy = datetime.now().date()
        print ("INGRESANDO FORMATO 12-B ==> " +str(fechahoy))
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> FORMATO 12-B ACTUALIZADOS A LA FECHA => " + str(fechahoy) + " Tabla [formato12b] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
       
        # if len(getformato12b()) == len(lista_formato_12b()) :
        #     print("ACTUALIZANDO FECHA A " + str(fechahoy) )
        #     con.getCur().execute("""
        #                 UPDATE formato12b SET
        #                     fecha_subida = %s
        #                 WHERE fecha_subida is null
        #             """, (
        #                     fechahoy,
        #             ))
        #     # con.getConn().commit()
        # else:
        
        # if len(getformato12b()) == len(lista_formato_12b(fechahoy)) :
        
        while len(getformato12b()) != len(lista_formato_12b(fechahoy)) :
            print("INGRESANDO")
            insertFormato12b(getformato12b(),fechahoy)
            print("DATOS A INGRESAR " + str(len(getformato12b())))
            print("LISTA INGRESADA " + str(len(lista_formato_12b(fechahoy))))

        print("DATOS A INGRESAR " + str(len(getformato12b())))
        print("LISTA INGRESADA " + str(len(lista_formato_12b(fechahoy))))
        print("------> PROYECTOS INGRESADOS AL FORMATO 12-B A LA FECHA DE " + str(fechahoy))

    # Iniciar
    __init__()
