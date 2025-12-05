import sys
from bs4 import BeautifulSoup
import sanitize
import codecs

def bs4Html(document):
    response = codecs.open(document, 'r', 'latin-1').read()

    ini = response.find('<body')
    fin = response.find('</body>')

    htmlstr = response[ini + 7 :fin]

    htmlstr = htmlstr.replace('<br>','').replace('&nbsp;','')
    htmlstr = " ".join(htmlstr.split())
    htmlstr = sanitize.deleteLineBreaks(htmlstr)
    htmlstr = htmlstr.replace('> <','><')
    htmlstr = htmlstr.replace('"','')
    soup = BeautifulSoup(htmlstr, 'html.parser')

    return soup
