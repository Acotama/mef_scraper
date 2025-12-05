def main(con):

    def Lista_Sector():
        con.getCur().execute("""
         select codigo_unico,categoria from tb_ejecucion_grupo where denominacion ='Funcion'
          and fecha =(select max(fecha) from tb_ejecucion_grupo where anio=date_part('year', current_date)) and anio =date_part('year', current_date);
        """)
        return con.getCur().fetchall()
        

    def Lista_Proyectos():
        con.getCur().execute("""SELECT id,cod_unif, sector FROM grli_pip_total_priori order by id""")
        return con.getCur().fetchall()


    def __init__():

        print('OBTENIENDO DATOS SECTOR...')

        data = Lista_Sector()

        for i in data:
            cod_unif = str(i['codigo_unico'])
            sector = str(i['categoria'])

            ########################################################################################
            ####################################### ACTUALIZAR CAMPOS ##############################
            con.getCur().execute(""" UPDATE grli_pip_total_priori SET sector = %s where cod_unif = %s """,
            (
                str(sector),
                str(cod_unif)
            ))

        con.getConn().commit()
        print('ACTUALIZADO...')
    # Iniciar
    __init__()
