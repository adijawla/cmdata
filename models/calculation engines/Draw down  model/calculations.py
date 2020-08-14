import time
def calcddm():
    # add code here
    #from chinacost import costmodel
    from ddm import calculation_engine
    

def calcInv():    
    from ddm.economic import Inventory 
    v = Inventory()
    v.calcall()

def calcchinacost():
    # add code here
    from chinacost import costmodel
    
