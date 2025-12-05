import os
import json
import requester
import reader
from datetime import datetime, timedelta
from decimal import Decimal

def main(con,years):

    def getProjects():

        con.getCur().execute(""" select cod_unif from vw_formato12b_proyecto""") 
        return con.getCur().fetchall()

    def dateExists_ET(fecha):
        con.getCur().execute("""
            SELECT * FROM tb_expediente_tecnico
            WHERE fecha_subida = %s
            """, (fecha,))    

        if len(con.getCur().fetchall()) > 0 :
            return True
        return False

    def __init__():
        data = getProjects()
        if not dateExists_ET(datetime.now().date()):
            for item in data:
                exp = requester.consulta_ex_de(item['cod_unif'])
                if len(exp[0]) > 0:
                    print("=============INGRESANDO==================")
                    con.getCur().execute("""
                        INSERT INTO tb_expediente_tecnico(
                            codigo_unico,
                            fec_program,
                            fec_actualizada,
                            fec_final,
                            estado_hito,
                            tipo,
                            fecha_subida,
                            resumen
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) """,
                        (
                            str(exp[0][0]['codigo_unico']),
                            exp[0][0]['fec_program'],
                            exp[0][0]['fec_actualizada'],
                            exp[0][0]['fec_final'],
                            exp[0][0]['estado_hito'],
                            str(exp[0][0]['tipo']),
                            str(datetime.now().date()),
                            str('SI')
                        )
                    )
                
                if len(exp[1]) > 0:
                    tipo = exp[1][0]['tipo']
                    data = exp[1]
                    data.pop(0) 
                    for items in data:
                        print("=============INGRESANDO DETALLE==================")
                        con.getCur().execute("""
                            INSERT INTO tb_expediente_tecnico(
                                codigo_unico,
                                fec_program,
                                fec_actualizada,
                                fec_final,
                                estado_hito,
                                tipo,
                                fecha_subida,
                                resumen,
                                etapa,
                                hito
                                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """,
                            (
                                str(items['COD_UNICO']),
                                items['FEC_PROGRAM'],
                                items['FEC_ACTUALIZADA'],
                                items['FEC_FINAL'],
                                items['ESTADO_HITO'],
                                str(tipo),
                                str(datetime.now().date()),
                                str('NO'),
                                str('ELABORACIÓN DE ET'),
                                items['DES_HITO']
                            )
                        )
                    con.getConn().commit()
                
            con.getConn().commit()
        else:
            print("DATOS YA ACTUALIZADOS DEL TB_EXPEDIENTE_TECNICO " + str(datetime.now().date()) + " Tabla [tb_expediente_tecnico]...")   
# Iniciar
    __init__()