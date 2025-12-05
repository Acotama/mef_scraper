import requests

def downloadFromConsultaAmigable(url,headers,destiny):
    print ("Objetivo [url]:  "+ str(url) )

    try:
        resp = requests.get(url, headers=headers,verify=False)
    except:
        print ("Fallo descargar archivo, verifique conexion a internet o enlace de descarga")
        exit(0)

    output = open(destiny, 'wb')
    output.write(resp.content)
    output.close()

