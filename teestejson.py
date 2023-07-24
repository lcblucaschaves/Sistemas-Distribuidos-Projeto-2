# -*- coding: utf-8 -*-
"""
Created on Sat Jul 22 02:00:29 2023

@author: Lucas
"""

import json

class Mensagem:
    def __init__(self, requisicao, tipo, chave, valor, timestamp):
        self.requisicao = requisicao
        self.chave = chave
        self.valor = valor
        self.timestamp = timestamp

        #TIMESTAMP EH POR CHAVE, ENTAO INCREMENTA A CADA PUT

def atualizaBase(baseDeDados, chave, valor, timestamp):
    chaveExiste = False 
    for item in baseDeDados:
        if item[0] == chave:
            chaveExiste = True
            item[1] = valor
            item[2] = timestamp
            break                
    
    if not chaveExiste:
        dado = [chave, valor, timestamp]
        baseDeDados.append(dado)
    
    print (baseDeDados)
    
baseDeDados = []

msg_aux = Mensagem("GET", "CLIENT", "chave teste", "1", "0")
msg = json.dumps(msg_aux.__dict__)

msg_teste = json.loads(msg)

print (msg_aux)
print (msg)
print (msg_teste)

atualizaBase(baseDeDados, msg_teste["chave"], msg_teste["valor"], msg_teste["timestamp"])
