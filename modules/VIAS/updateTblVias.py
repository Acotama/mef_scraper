from datetime import datetime
import reader

def main(con):
    years = datetime.now().year 
    def __init__():
        print ('INCIANDO...')

        print ('ACTUALIZANDO MATENIMIENTOS DE VIAS PERIODICO...')
        document = './downloads/'+ str(years) +'/EjecucionPresupuestal/Vias/Periodico/' + str(datetime.now().date()) + '.html'

        data = reader.bs4Html(document)
        trs = data.find('table', class_="Data").contents
        update(trs, 'PER')

        print ('ACTUALIZANDO MANTENIMIENTOS DE VIAS RUTINARIO...')
        document = './downloads/'+ str(years) +'/EjecucionPresupuestal/Vias/Rutinario/' + str(datetime.now().date()) + '.html'

        data = reader.bs4Html(document)
        trs = data.find('table', class_="Data").contents
        update(trs, 'RUT')

        con.getConn().commit()

    def tipMant(tipo):
        if tipo == 'PER':
            return 'MANTENIMIENTO PERIODICO'
        elif tipo == 'RUT':
            return 'MANTENIMIENTO RUTINARIO'

    def exists(codigo):
        con.getCur().execute("""
            SELECT * FROM grli_mantenimiento_via
            WHERE grli_mantenimiento_via.cod_mant = %s
            AND grli_mantenimiento_via.estado = '1'
        """, (str(codigo),) )

        file = con.getCur().fetchall()
        if len(file) > 0 :
            return True
        return False

    def update(data, tip):
        i = 0
        for row in range(int(len(data)/2)):
            first_row  = data[row+i]
            second_row = data[row+i+1].contents[0].text

            nombre          = first_row.contents[0].text.strip()
            codigo_financ   = nombre.split(':')[0].strip()
            pia             = first_row.contents[1].text.replace(",", "").strip()
            pim             = first_row.contents[2].text.replace(",", "").strip()
            certificacion   = first_row.contents[3].text.replace(",", "").strip()
            comp_anual      = first_row.contents[4].text.replace(",", "").strip()
            ate_comp_anual  = first_row.contents[5].text.replace(",", "").strip()
            devengado       = first_row.contents[6].text.replace(",", "").strip()
            girado          = first_row.contents[7].text.replace(",", "").strip()
            av_financ       = first_row.contents[8].text.replace(",", "").strip()


            codigo = tip+str(datetime.now().year)+codigo_financ

            cantidad = second_row[second_row.find('Cantidad: '):second_row.find('Unidad')].split(':')[1].strip()
            unidad_medida = second_row[second_row.find('Unidad'):second_row.find('Avance')].split(':')[1].strip()
            av_fisico = second_row[second_row.find('Avance'):second_row.find('(')].split(':')[1].strip()
            av_fisico_km = second_row[second_row.find('(')+1:second_row.find(')')-1].strip()


            if exists(codigo):
                print("ACTUALIZANDO MANTENIMIENTO DE VIA ["+ str(codigo_financ) + "]")
                con.getCur().execute("""
                    UPDATE grli_mantenimiento_via
                    SET
                        activ = %s,
                        tip_mant = %s,
                        mfis_pro = %s,
                        mfin_pro = %s,
                        ejec_fisico = %s,
                        av_fisico = %s,
                        deven_finan = %s,
                        av_finan = %s,
                        anio = %s,
                        f_afinanc = %s,
                        u_medida = %s,
                        estado = %s
                    WHERE cod_mant = %s
                  """, (
                    str(nombre),
                    str(tipMant(tip)),
                    str(cantidad),
                    str(pim),
                    str(av_fisico_km),
                    str(av_fisico),
                    (int(devengado) if len(devengado)>0 else 0),
                    str(av_financ),
                    str(years),
                    str(datetime.now().date()),
                    str(unidad_medida),
                    '1',
                    str(codigo)
                  ) )

            else:
                print("AGREGANDO MANTENIMIENTO DE VIA ["+ str(codigo_financ) + "]")
                con.getCur().execute("""
                    INSERT INTO grli_mantenimiento_via(
                        dep,
                        cod_dep,
                        activ,
                        tip_mant,
                        mfis_pro,
                        mfin_pro,
                        ejec_fisico,
                        av_fisico,
                        deven_finan,
                        av_finan,
                        anio,
                        f_afinanc,
                        u_medida,
                        cod_mant,
                        estado)
                      VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                  """, (
                    'LIMA',
                    '15',
                    str(nombre),
                    str(tipMant(tip)),
                    str(cantidad),
                    str(pim),
                    str(av_fisico_km),
                    str(av_fisico),
                    (int(devengado) if len(devengado)>0 else 0),
                    str(av_financ),
                    str(years),
                    str(datetime.now().date()),
                    str(unidad_medida),
                    str(codigo),
                    '1'
                  ) )

            i+=1



    
    # Iniciar
    __init__()
