import datetime
import reader

def main(con):
    def exists(fecha):
        con.getCur().execute(""" SELECT * FROM inf_financiera_rank_activ WHERE inf_financiera_rank_activ.fecha = %s """, (str(fecha),) )
        Finance = con.getCur().fetchall()
        return len(Finance)

    def insert(data, fecha):
        puesto = 0
        for row in data:
            puesto+=1
            #Nombre Proyecto
            cod_gore = row.contents[0].text

            cod = cod_gore.split(':')[0]
            gore = cod_gore.split(':')[1]

            pia = row.contents[1].text.replace(",", "")
            pia = pia.replace(".", ",")

            pim = row.contents[2].text.replace(",", "")
            pim = pim.replace(".", ",")

            certif = row.contents[3].text.replace(",", "")
            certif = certif.replace(".", ",")

            comp_anual = row.contents[4].text.replace(",", "")
            comp_anual = comp_anual.replace(".", ",")

            at_comp_anual = row.contents[5].text.replace(",", "")
            at_comp_anual = at_comp_anual.replace(".", ",")

            deveng = row.contents[6].text.replace(",", "")
            deveng = deveng.replace(".", ",")

            girado = row.contents[7].text.replace(",", "")
            girado = girado.replace(".", ",")

            avance = row.contents[8].text.replace(",", "")

            con.getCur().execute("""
                INSERT INTO inf_financiera_rank_activ(cod,gore,pia,pim,certif,comp_anual,at_comp_anual,deveng,girado,avance,puesto,fecha)
                VALUES(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
            """, (
                    str(cod).strip(),
                    str(gore).strip(),
                    str(pia).strip(),
                    str(pim).strip(),
                    str(certif).strip(),
                    str(comp_anual).strip(),
                    str(at_comp_anual).strip(),
                    str(girado).strip(),
                    str(deveng).strip(),
                    str(avance).strip(),
                    str(puesto).strip(),
                    str(fecha).strip()
                )
            )

    def __init__():
        print 'Iniciando...'
        fecha = str(datetime.datetime.now().date())

        if exists( fecha ) == 0:
            document = './downloads/ConsultaAmigable/Rank/Actividades/' + fecha + '.html'

            data = reader.bs4Html(document)
            trs = data.find('table', class_="Data").contents
            insert(trs,fecha)

            print 'Guardando...'
        
        else:
            print 'Ya se Ingreso Informacion de Hoy'

    __init__()
