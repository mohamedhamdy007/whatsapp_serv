import random
import string

def random_code():
    letters = ''.join(random.choices(string.ascii_lowercase, k=4))
    digits = ''.join(random.choices(string.ascii_lowercase, k=4))
    #return f"wa{letters}_{digits}"
    return "wa_intg1122"

#print(random_code())