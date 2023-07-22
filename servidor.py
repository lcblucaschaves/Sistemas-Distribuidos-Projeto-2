# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 08:46:25 2023

@author: lucas.chaves
"""

import socket
import threading

class Mensagem:
    def __init__(self, requisicao, chave, valor, timestamp):
        self.requisicao = requisicao
        self.chave = chave
        self.valor = valor
        self.timestamp = timestamp

class Servidor:
    def __init__ (self, host, port, liderHost, liderPort):
        self.host = host
        self.port = int (port)
        self.liderHost = liderHost
        self.liderPort = int (liderPort)
        self.baseDeDados = []
        self.listaServidores = [10097,10098,10099]
        self.listaServidores.remove(self.port)
        
        if (self.liderHost == self.host) and (self.liderPort == self.port):
            self.souLider = True
            print ("sou lider")
        else:
            self.souLider = False
            print ("não sou lider")
        
        try:
            self.servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.servidor.bind((self.host, self.port))
        except socket.error as e: #Tratamento de erros
            print (f"erro na criacao do socket erro {e}")
        
    def join(self):
        self.servidor.listen()   #servidor aguardando conexoes      
        
        while True:
            #conexoes realizadas
            conexao, aux_addr = self.servidor.accept() 
            porta_real = conexao.recv(1024).decode()
            addr = (aux_addr[0], int(porta_real))
            conexao.send("OK".encode())
            
            #iniciando a thread para a funcao multithreads
            thread_processos = threading.Thread(target=self.multiThread, args=[conexao, addr])            
            thread_processos.start()
            thread_processos.join()
            
    def multiThread (self, conexao, addr):
        req = conexao.recv(1024).decode() #aguardando requisicao
        print (f"Cliente {addr[0]}:{addr[1]} requisitou {req}")
        conexao.shutdown(socket.SHUT_RDWR)
        conexao.close()
        return
    
    def atualizaBase(self, chave, valor, timestamp):
        chaveExiste = False 
        for item in self.baseDeDados:
            if item[0] == chave:
                chaveExiste = True
                if item[2] < timestamp:
                    item[1] = valor
                    item[2] = timestamp
                break
        
        if not chaveExiste:
            dado = [chave, valor, timestamp]
            self.baseDeDados.append(dado)
    
    def PUT(self, chave, valor, timestamp, conexao):
        if self.souLider:
            self.atualizaBase(chave, valor, timestamp)            
            try:
                servidor1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                servidor2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                
                servidor1.connect(('127.0.0.1', self.listaServidores[0]))
                servidor2.connect(('127.0.0.1', self.listaServidores[1]))
                
                msg = Mensagem("REPLICATION", "SERVER", chave, valor)
                
                servidor1.send(msg.encode())
                servidor1.recv(1024) #REPLICATION OK
                
                servidor2.send(msg.encode())
                servidor2.recv(1024) #REPLICATION OK
                
                conexao.send("PUT OK".encode())
                
                servidor1.close()
                servidor2.close()
            except socket.error as e: #Tratamento de erros
                print (f"erro na criacao do socket erro {e}")
            
        else:
            servidorLider = socket.socket(socket.AF_INET, socket.SOCK_STREAM)            
            servidorLider.connect((self.liderHost, self.liderPort))
            servidorLider.send(msg.encode())
            servidorLider.recv(1024) #REPLICATION OK
            
            conexao.send("PUT OK".encode())
                    
    
def main():
    h = input ("Digite o IP: ")
    p = input ("Digite a Porta: ")
    lh = input ("Digite o IP do Lider: ")
    lp = input ("Digite a Porta do Lider: ")
    
    server = Servidor(h, p, lh, lp)
    server.join()
    
main()