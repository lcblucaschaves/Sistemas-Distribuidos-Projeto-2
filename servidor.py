# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 08:46:25 2023

@author: lucas.chaves
"""

import socket
import threading
import json

class Mensagem:
    def __init__(self, requisicao, chave, valor, timestamp):
        self.requisicao = requisicao
        self.chave = chave
        self.valor = valor
        self.timestamp = timestamp

        #TIMESTAMP EH POR CHAVE, ENTAO INCREMENTA A CADA PUT
        
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
        else:
            self.souLider = False
        
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
            
    def multiThread (self, conexao, addr):
        req = conexao.recv(1024).decode() #aguardando requisicao
        msg = json.loads(req)   
        
        if msg["requisicao"] == "PUT":
            if self.souLider:
                print (f"Cliente [{addr[0]}]:[{addr[1]}] PUT key: [{msg['chave']}] value: [{msg['valor']}]")
            else:
                print (f"Encaminhando PUT key: [{msg['chave']}] value: [{msg['valor']}]")
            self.PUT(conexao, addr, req)       
        
        elif msg["requisicao"] == "REPLICATION":
            print (f"REPLICATION key: [{msg['chave']}] value: [{msg['valor']}] ts: [{msg['timestamp']}]")
            self.REPLICATION(conexao, msg)
                
        elif msg["requisicao"] == "GET":
            self.GET(conexao, addr, msg)
           
        else:
            print ("REQUISICAO NAO EXISTE")
        
        conexao.shutdown(socket.SHUT_RDWR)
        conexao.close()
        return
    
    def atualizaBase(self, chave, valor, timestamp):
        chaveExiste = False 
        for item in self.baseDeDados:
            if item[0] == chave:
                chaveExiste = True
                item[1] = valor
                item[2] = timestamp
                return               
        
        if not chaveExiste:
            dado = [chave, valor, timestamp]
            self.baseDeDados.append(dado)
            
    def buscaBase(self, chave, timestamp):
        for item in self.baseDeDados:
            if item[0] == chave:
                return item
        return []
    
    def PUT(self, conexao, addr, req): 
        if self.souLider:
            msg = json.loads(req)
            self.atualizaBase(msg["chave"], msg["valor"], msg["timestamp"])            
            try:
                servidor1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                servidor2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                
                port = str(self.port)
                
                servidor1.connect(('127.0.0.1', self.listaServidores[0]))
                servidor1.send(port.encode())
                servidor1.recv(1024)
                
                servidor2.connect(('127.0.0.1', self.listaServidores[1]))
                servidor2.send(port.encode())
                servidor2.recv(1024)
                
                msg_server_aux = Mensagem("REPLICATION", msg["chave"], msg["valor"], msg["timestamp"])
                msg_server = json.dumps(msg_server_aux.__dict__)
                
                servidor1.send(msg_server.encode())
                s1_aux = servidor1.recv(1024).decode() #REPLICATION OK
                
                servidor2.send(msg_server.encode())
                s2_aux = servidor2.recv(1024).decode() #REPLICATION OK
                
                s1 = json.loads(s1_aux)
                s2 = json.loads(s2_aux)
                
                if s1["requisicao"] == "REPLICATION_OK" and s2["requisicao"] == "REPLICATION_OK":
                    print (f"Enviando PUT_OK ao Cliente [{addr[0]}]:[{addr[1]}] da key: [{msg['chave']}] ts: [{msg['timestamp']}]")
                    msg_aux = Mensagem("PUT_OK", msg["chave"], msg["valor"], msg["timestamp"])
                else:
                    msg_aux = Mensagem("PUT_ERROR", msg["chave"], msg["valor"], msg["timestamp"])
                
                resp = json.dumps(msg_aux.__dict__)   
                conexao.send(resp.encode())
                
                servidor1.close()
                servidor2.close()
            except socket.error as e: #Tratamento de erros
                print (f"erro {e}")
            
        else:
            servidorLider = socket.socket(socket.AF_INET, socket.SOCK_STREAM)            
            servidorLider.connect((self.liderHost, self.liderPort))
            
            port = str(addr[1])
            servidorLider.send(port.encode())
            servidorLider.recv(1024)
            
            servidorLider.send(req.encode())            
            sl = servidorLider.recv(1024).decode()             
            conexao.send(sl.encode())
                    
    def REPLICATION(self, conexaoServidor, msg):
        try:           
            self.atualizaBase(msg["chave"], msg["valor"], msg["timestamp"])
            
            msg_aux = Mensagem("REPLICATION_OK", msg["chave"], msg["valor"], msg["timestamp"])
            resp = json.dumps(msg_aux.__dict__)     
            
            conexaoServidor.send(resp.encode())
        except socket.error as e: #Tratamento de erros
            print (f"erro {e}")
            
    def GET(self, conexao, addr, msg):
        try:
            item = self.buscaBase(msg["chave"], msg["timestamp"])
            
            if len (item) != 0:
                msg_aux = Mensagem("GET_OK", item[0], item[1], item[2])
            else:
                msg_aux = Mensagem("GET_NULL", "NULL", "NULL", msg["timestamp"])
            
            
            resp = json.dumps(msg_aux.__dict__)
            print (f"Cliente [{addr[0]}]:[{addr[1]}] GET key: [{msg['chave']}] ts: [{msg['timestamp']}]. Meu ts é [ts server], portanto devolvendo [{msg_aux.valor}]")
            conexao.send(resp.encode())
        except socket.error as e: #Tratamento de erros
            print (f"GET erro {e}")
    
def main():
    h = input ("Digite o IP: ")
    p = input ("Digite a Porta: ")
    lh = input ("Digite o IP do Lider: ")
    lp = input ("Digite a Porta do Lider: ")
    
    server = Servidor(h, p, lh, lp)
    server.join()
    
main()