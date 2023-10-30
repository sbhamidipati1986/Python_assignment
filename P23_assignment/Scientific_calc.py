from future.moves import tkinter
from tkinter import *
import math

def bc(val):
    e = entry.get()  # getting the value
    res = " "

    try:
        if val == "π":
            res = math.pi

        elif val == "e":
            res = 2.7182818284590452353602874713527

        elif val == "1/x":
            res = e**-1

        elif val == "CE":
            entry.delete(0, "end")

        elif val == "C":
            e = e[0:len(e) - 1]  # deleting the last entered value
            entry.delete(0, "end")
            entry.insert(0, e)
            return

        elif val == "x\u00B2":
            res = eval(e) ** 2

        elif val == "x\u00B3":
            res = eval(e) ** 3

        elif val == "|x|":
            res = abs(eval(e))

        elif val == "exp":
            entry.delete(0, "end")
            entry.insert("end",e+ '.e+')
            res = e+ '**1*10**'
            return

        elif val == "√":
            res = math.sqrt(eval(e))

        elif val == "n!":
            res = math.factorial(eval(e))

        elif val == "x\u02b8":
            entry.insert("end", "**")
            return

        elif val == "10\u02e3":
            res = 10**eval(e)

        elif val == "ln":
            res = math.log2(eval(e))

        elif val == "log":
            res = math.log10(eval(e))

        elif val == chr(8731):
            res = eval(e) ** (1 / 3)

        elif val == chr(247):
            entry.insert("end", "/")
            return

        elif val == "=":
            res = eval(e)

        else:
            entry.insert("end", val)
            return

        entry.delete(0, "end")
        entry.insert(0, res)

    except SyntaxError:
        pass
wi=Tk()
wi.title('Calculator')
wi.geometry("680x486+100+100")
wi.config(bg="white")
entry = tkinter.Entry(wi,relief=SUNKEN, font=("arial", 20, "bold"),  bd=29, width=28)
entry.grid(row=0, column=0, columnspan=6)
bu = ["π","e","1/x","CE","C","x\u00B2","x\u00B3",'|x|','exp','%',"√","(", ")","n!","/","x\u02b8","7","8","9","*","10\u02e3"
      ,"4","5","6","-","log","1","2","3","+","ln","+/-","0",".","="]
r = 1
c = 0
# Loop to get the buttons on window
for i in bu:
    # Buttons
    bi = tkinter.Button(wi, width=5, height=2, bd=2, text=i,
                            font=("arial", 18, "bold"), command=lambda bi=i: bc(bi))
    bi.grid(row=r, column=c, pady=1)
    c += 1
    if c > 4:
        r += 1
        c = 0
wi.mainloop()