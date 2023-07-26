# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 09:07:54 2023

@author: lucas.chaves
"""

import socket
import random
import json

class Mensagem:
    def __init__(self, requisicao, chave, valor, timestamp):
        self.requisicao = requisicao
        self.chave = chave
        self.valor = valor
        self.timestamp = timestamp

        #TIMESTAMP EH POR CHAVE, ENTAO INCREMENTA A CADA PUT

class Cliente:
    def __init__ (self, host, port):
        self.host = host
        self.port = int (port)
        self.servidores = []
        self.rngServ = 0        
        self.clientTS = 0
        self.chavesTS = []
    
    def INIT(self):
        for i in range(0, 3):
            hserver = input (f"Digite o IP do {i + 1}º servidor: ")
            pserver = input (f"Digite a porta do {i + 1}º servidor: ")            
            self.servidores.append((hserver, int (pserver)))
    
    def INIT_CONNECT(self):
        try:
            self.cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.rngServ = random.randint(0, 2)
            self.cliente.connect((self.servidores[self.rngServ][0], self.servidores[self.rngServ][1]))   #conecta com o server 
            aux = str(self.port)                      #envia a porta real
            self.cliente.send(aux.encode())
            self.cliente.recv(1024)
        except socket.error as e: #Tratamento de erros
            print (f"erro na criacao do socket erro {e}")
    
    def PUT(self, chave, valor):
        try:            
            msg_aux = Mensagem("PUT", chave, valor, self.clientTS)
            msg = json.dumps(msg_aux.__dict__)            
            
            self.cliente.send(msg.encode())
            resp_aux = self.cliente.recv(1024).decode()
            resp = json.loads(resp_aux)
            
            if resp["requisicao"] == "PUT_OK":
                print (f"\nPUT_OK key: [{resp['chave']}] value: [{resp['valor']}] timestamp: [{resp['timestamp']}] realizada no servidor [{self.servidores[self.rngServ][0]}:{self.servidores[self.rngServ][1]}]")
                
                dado = [resp['chave'], resp['timestamp']]
                self.chavesTS.append(dado)
            
            else:
                print (f"\nPUT_ERROR key: [{resp['chave']}] value: [{resp['valor']}] timestamp: [{resp['timestamp']}] realizada no servidor [{self.servidores[self.rngServ][0]}:{self.servidores[self.rngServ][1]}]")
            
            self.cliente.shutdown(socket.SHUT_RDWR)
            self.cliente.close()
            
            self.clientTS = self.clientTS + 1
                        
        except socket.error as e: #Tratamento de erros
            print (f"erro {e}")
    
    def GET(self, key):
        try:            
            msg_aux = Mensagem("GET", key, "NULL", "0")            
            for item in self.chavesTS:
                if item[0] == key:
                    msg_aux = Mensagem("GET", item[0], "NULL", item[1])
            
            msg = json.dumps(msg_aux.__dict__)            
            self.cliente.send(msg.encode())
            
            resp_aux = self.cliente.recv(1024).decode()
            resp = json.loads(resp_aux)
            
            if resp["requisicao"] == "GET_ERROR":
                print ("\nTRY_OTHER_SERVER_OR_LATER")
            else: 
                print (f"\nGET key: [{key}] value: [{resp['valor']}] obtido do servidor [{self.servidores[self.rngServ][0]}:{self.servidores[self.rngServ][1]}], meu timestamp [{msg_aux.timestamp}] e do servidor [{resp['timestamp']}]")
                
                if resp["valor"] != "NULL":
                    dado = [resp['chave'], resp['timestamp']]
                    self.chavesTS.append(dado)
                
            self.cliente.shutdown(socket.SHUT_RDWR)
            self.cliente.close()
                        
        except socket.error as e: #Tratamento de erros
            print (f"erro na conexao erro {e}")
        
def menu():
    print ("\nSelecione uma opção (Digite um Numero):")
    print ("0. INIT do cliente")
    print ("1. PUT de chave")
    print ("2. GET de chave")
    print ("3. SAIR")
    
def main():
    h = input ("Digite o IP: ")
    p = input ("Digite a Porta: ")
    
    client = Cliente (h, p)
    while True:        
        menu()
        escolha = input ("Digite o número da opção desejada: ")
    
        if escolha == "0":
            print ("\nOpção INIT selecionada")
            client.INIT()          
    
        elif escolha == "1":
            print ("\nOpção PUT selecionada")  
            
            key = input ("Digite a Chave: ")
            value = input ("Digite o valor: ")
            
            client.INIT_CONNECT()          
            client.PUT(key, value)
                    
        elif escolha == "2":
            print ("\nOpção GET selecionada")
            
            key = input ("Digite a Chave: ")
            
            client.INIT_CONNECT()
            client.GET(key)            
                    
        elif escolha == "3":
            break
    
        else:
            print ("\nOpção inválida. Por favor, selecione uma opção válida.\n")
    
main()