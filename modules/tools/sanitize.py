def deleteLineBreaks(string):
  return string.replace('\r', '').replace('\n', '')

def deleteNonLineBreaks(string):
  return string.replace(u'\xa0', '')

def sanitizeDoc(string):
    return string.replace('\r', '').replace('\n', '').replace(u'\xa0', '')
