

def domain(mail):
    return mail.split("@")[-1]

def contains(st):
    if 'dog' in st.lower():
        return True
    else:
        return False

def countDog(st):
    val=0
    for word in st.lower().split():
        if 'dog' == word:
            val+=1
    return val

def caught_speeding(speed ,is_birthday):
    if is_birthday:
        speed-=5
    if speed<=60:
        return "No Ticket"
    elif speed>=61 and speed<=80:
        return "Small Ticket"
    else:
        return "Big Ticket"

str ="is there dog in here dog dog ?"

print(caught_speeding(81,True))
print(caught_speeding(81,False))

