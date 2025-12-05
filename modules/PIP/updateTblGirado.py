import reader
from datetime import datetime
import codecs

def main(con, years):

    def insert(data):
        con.getCur().execute("DELETE FROM grli_pip_total_girado where anio::int = %s", ( str(years), ) )
        for row in data:
            #Nombre Proyecto
            nombreProyecto = row.contents[0].text
            #Codigo del proyecto
            codigo = nombreProyecto.split(':')[0]
            #Nombre del proyecto
            nombre = nombreProyecto.split(':')[1]

            #Girado
            girado = row.contents[7].text.replace(",", "")
            girado = girado.replace(".", ",")

            con.getCur().execute("""INSERT INTO grli_pip_total_girado VALUES(%s,%s,%s)""", (
                str(codigo),
                str(years),
                str(girado)
            ))

        print ('Guardando -> Tabla [grli_pip_total_girado]...')
        con.getConn().commit()

    def __init__():

        document = './downloads/'+ str(years) +'/EjecucionPresupuestal/Proyectos/' + str(datetime.now().date()) + '.html'
        data = reader.bs4Html(document)
        trs = data.find('table', class_="Data").contents
        insert(trs)

    # Iniciar
    __init__()
