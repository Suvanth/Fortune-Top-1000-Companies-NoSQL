
stringtext = "the apple is  "

negfloat= "($900.354)" 
negfloat = negfloat.replace("($", "-")
negfloat = float(negfloat.replace(")", ""))
print(negfloat)