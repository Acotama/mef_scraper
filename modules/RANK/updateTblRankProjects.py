from datetime import datetime
import reader

def main(con,anio):

    def exists(fecha):
        con.getCur().execute(""" SELECT * FROM inf_financiera_rank WHERE inf_financiera_rank.fecha = %s and anio = %s """, (str(fecha),anio) )
        Finance = con.getCur().fetchall()
        return len(Finance)

    def insert(data, fecha, categoria):
        puesto = 0
        for row in data:
            puesto+=1
            #Nombre Proyecto
            cod_gore = row.contents[0].text

            cod = cod_gore.split(':')[0]
            gore = cod_gore.split(':')[1]

            pia = row.contents[1].text.replace(",", "")
            pia = pia.replace(".", ",")
            pia = pia if len(pia) != 0 else 0

            pim = row.contents[2].text.replace(",", "")
            pim = pim.replace(".", ",")
            pim = pim if len(pim) != 0 else 0

            certif = row.contents[3].text.replace(",", "")
            certif = certif.replace(".", ",")
            certif = certif if len(certif) != 0 else 0

            comp_anual = row.contents[4].text.replace(",", "")
            comp_anual = comp_anual.replace(".", ",")
            comp_anual = comp_anual if len(comp_anual) != 0 else 0

            at_comp_anual = row.contents[5].text.replace(",", "")
            at_comp_anual = at_comp_anual.replace(".", ",")
            at_comp_anual = at_comp_anual if len(at_comp_anual) != 0 else 0

            deveng = row.contents[6].text.replace(",", "")
            deveng = deveng.replace(".", ",")
            deveng = deveng if len(deveng) != 0 else 0

            girado = row.contents[7].text.replace(",", "")
            girado = girado.replace(".", ",")
            girado = girado if len(girado) != 0 else 0

            avance = row.contents[8].text.replace(",", "")
            avance = avance if len(avance) != 0 else 0

            con.getCur().execute("""
                INSERT INTO inf_financiera_rank(cod,gore,pia,pim,certif,comp_anual,at_comp_anual,deveng,girado,avance,puesto,fecha,categoria,anio)
                VALUES(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
            """, (
                    str(cod).strip(),
                    str(gore).strip(),
                    str(pia).strip(),
                    str(pim).strip(),
                    str(certif).strip(),
                    str(comp_anual).strip(),
                    str(at_comp_anual).strip(),
                    str(deveng).strip(),
                    str(girado).strip(),
                    str(avance).strip(),
                    str(puesto).strip(),
                    str(fecha).strip(),
                    str(categoria),
                    anio
                )
            )

    def __init__():
        print("INICIANDO...")
        fecha = str(datetime.now().date())
        if exists( fecha ) == 0:
            #RANKING PLIEGO REGIONAL
            data = reader.bs4Html('./downloads/'+ str(anio) +'/Rank/Proyectos/' + str(fecha) + '.html')
            trs = data.find('table', class_="Data").contents
            insert(trs,fecha,"PLIEGO")
            #RANKING SECTOR-REGIONAL 
            datasectorregional = reader.bs4Html('./downloads/'+ str(anio) +'/Rank/Proyectos/Sector/Regional/' + str(fecha) + '.html')
            trs = datasectorregional.find('table', class_="Data").contents
            insert(trs,fecha,"SECTOR-REGIONAL")
            #RANKING SECTOR-REGIONAL LIMA
            # datasecreglima = reader.bs4Html('./downloads/'+ str(anio) +'/Rank/Proyectos/Sector/RegionLima/' + str(fecha) + '.html')
            # trs = datasecreglima.find('table', class_="Data").contents
            # insert(trs,fecha,"SECTOR-REGIONAL-LIMA")

            print("GUARDANDO...")
            con.getConn().commit()
        else:
            print("YA SE INGRESO INFORMACION DE HOY")

    __init__()
