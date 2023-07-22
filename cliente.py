# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 09:07:54 2023

@author: lucas.chaves
"""

import socket
import threading
import random

class Mensagem:
    def __init__(self, requisicao, tipo, chave, valor):
        self.requisicao = requisicao
        self.tipo = tipo
        self.chave = chave
        self.valor = valor
        
        #TIMESTAMP EH POR CHAVE, ENTAO INCREMENTA A CADA PUT

class Cliente:
    def __init__ (self, host, port):
        self.host = host
        self.port = int (port)
        self.servidores = []        
    
    def INIT(self):
        for i in range(0, 3):
            hserver = input (f"Digite o IP do {i + 1}º servidor: ")
            pserver = input (f"Digite a porta do {i + 1}º servidor: ")            
            self.servidores.append((hserver, int (pserver)))
    
    def INIT_CONNECT(self):
        try:
            self.cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            i = random.randint(0, 2)
            self.cliente.connect((self.servidores[i][0], self.servidores[i][1]))   #conecta com o server 
            aux = str(self.port)                      #envia a porta real
            self.cliente.send(aux.encode())
            self.cliente.recv(1024)
        except socket.error as e: #Tratamento de erros
            print (f"erro na criacao do socket erro {e}")
        
    def GET(self):
        try:
            self.cliente.send("GET".encode())
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
            client.INIT_CONNECT()                     
                    
        elif escolha == "2":
            print ("\nOpção GET selecionada")
            client.INIT_CONNECT()
            client.GET()            
                    
        elif escolha == "3":
            break
    
        else:
            print ("\nOpção inválida. Por favor, selecione uma opção válida.\n")
    
main()