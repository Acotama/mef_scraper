def main(con):

    def __init__():

        print('OBTENIENDO DATOS FUENTE...')
        con.getCur().execute("""SELECT id, ger_direc FROM grli_pip_total_priori order by id""")
        curi = con.getCur().fetchall()

        for i in curi:
            idPIP = int(i['id'])
            ger_direc = i['ger_direc']
            ########################################################################################
            ####################################### ACTUALIZAR CAMPOS ##############################

            con.getCur().execute(""" UPDATE grli_pip_total_priori SET fuente = %s where id = %s """,
            (
                "SISTEMA DE SEGUIMIENTO DE INVERSIONES(SSI)-MEF/CONSULTA AMIGABLE-MEF/"+ str(ger_direc),
                str(idPIP)
            ))

        con.getConn().commit()

    # Iniciar
    __init__()
