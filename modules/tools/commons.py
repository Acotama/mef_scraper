from decimal import Decimal

def isNumber(num):
    try:
        int(num)
        return True
    except ValueError:
        return False

def twoDec(num):
    num = Decimal(num).quantize(Decimal('1.00'))
    return num
