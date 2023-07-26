# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 09:07:54 2023

@author: lucas.chaves

Este eh um cliente simples que interage com tres servidores atraves de sockets TCP.
O cliente pode executar operacoes PUT e GET, onde PUT é usado para armazenar um valor em um servidor 
e GET eh usado para recuperar o valor associado a uma chave especifica.

O cliente mantem uma lista dos servidores e conecta-se a um deles aleatoriamente sempre que uma operacao eh executada.
O cliente utiliza a classe Mensagem no envio das requisições, chaves e valores

Alem da biblioteca socket para comunicação de rede, temos biblioteca json para serializacao de dados e a random para selecionar o
servidor aleatoriamente.

Classes:
- Mensagem: representa a estrutura de mensagem para PUT e GET. Armazena a requisicao, a chave, o valor e o timestamp.
- Cliente: A classe principal do cliente que lida com a interação com os servidores.

Funcoes:
- INIT: Inicializa a lista de servidores, solicitando ao usuário o IP e a porta de cada servidor.
- INIT_CONNECT: Conecta-se a um dos servidores da lista aleatoriamente, enviando a porta em uso pelo cliente para o servidor.
- PUT: Executa a operacao PUT, enviando uma mensagem ao servidor com a chave e o valor. Recebe uma resposta do servidor e atualiza a lista de timestamps das chaves.
- GET: Executa a operacao GET, enviando uma mensagem ao servidor com a chave e o timestamp associado a ela. Recebe uma resposta do servidor e atualiza a lista de timestamps das chaves.
- main: Inicia o cliente, solicitando o IP e a porta do cliente, e apresenta um menu interativo para o usuario escolher: INIT, PUT, GET ou SAIR.

O programa permite que o usuário execute operações PUT e GET em servidores distintos a cada chamada, usando a conexão TCP.
"""

#IMPORTANDO BIBLIOTECAS
import socket
import random
import json

"""
Classe Mensagem: representa a estrutura de mensagem para PUT e GET. Armazena a requisicao, a chave, o valor e o timestamp.
"""
class Mensagem:
    def __init__(self, requisicao, chave, valor, timestamp):
        self.requisicao = requisicao
        self.chave = chave
        self.valor = valor
        self.timestamp = timestamp

"""
CLasse Cliente: classe principal do cliente que lida com a interacao com os servidores.
    Recebe IP e Porta na inicializacao

MECANICA DO CLIENTE:
    Durante o INIT do cliente, armazena numa lista o IP e Porta do servidor (na variavel servidores). 
    Durante qualquer requisicao, utiliza a variavel rngServ, que eh um numero aleatorio entre 0 e 2 que representa
    uma posicao na lista de servidores, ou seja, representa 1 servidor
    O cliente possui uma variavel clientTS que representa seu timestamp, comecao em 0 e que incrementa a cada PUT
    Para cada requisicao, o cliente liga um socket e encerra ele no fim da requisicao
    O durante os PUTs e GETs, o cliente armazena as chaves e os timestamps que conhece dessas chaves 
    se eh a primeira vez em que ve a chave, ou ela nao existe, o timestamp eh 0. Caso contrario, eh o enviado pelo servidor
"""
class Cliente:
    def __init__ (self, host, port):
        self.host = host
        self.port = int (port)
        self.servidores = []  # Lista para armazenar tuplas com o IP e a porta dos servidores disponiveis
        self.rngServ = 0      # Indice do servidor escolhido aleatoriamente
        self.clientTS = 0     # Timestamp do cliente
        self.chavesTS = []    # Lista para armazenar chaves e seus respectivos timestamps

"""
Funcao INIT: Inicializa a lista de servidores, solicitando ao usuario o IP e a porta de cada servidor.
"""
    def INIT(self):
        for i in range(0, 3):
            hserver = input (f"Digite o IP do {i + 1}º servidor: ")
            pserver = input (f"Digite a porta do {i + 1}º servidor: ")            
            self.servidores.append((hserver, int (pserver)))

"""
Funcao INIT_CONNECT: Conecta-se a um dos servidores da lista aleatoriamente, enviando a porta em uso pelo cliente para o servidor.
    Aqui eh utilizado a mesma logica de porta real do EP 1. Como tive problemas em dar bind da porta no socket do cliente e eu
    queria mostrar no terminal qual Porta esta fazendo a requisicao, eu usei o mesmo esquema.
    As portas sao aleatorias e a Porta Real funciona como um "ID".
"""    
    def INIT_CONNECT(self):
        try:
            self.cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.rngServ = random.randint(0, 2)
            self.cliente.connect((self.servidores[self.rngServ][0], self.servidores[self.rngServ][1]))   # conecta com o server 
            aux = str(self.port)                                                                         # envia a porta real
            self.cliente.send(aux.encode())
            self.cliente.recv(1024)
        except socket.error as e: # Tratamento de erros
            print (f"erro na criacao do socket erro {e}")

"""
Funcao PUT: Recebe uma chave, um valor e executa a operação PUT, enviando uma mensagem ao servidor com a chave e o valor.
    Recebe uma resposta do servidor e atualiza a lista de timestamps das chaves.
"""
    def PUT(self, chave, valor):
        try:
            #Converte a mensagem para um JSON
            msg_aux = Mensagem("PUT", chave, valor, self.clientTS)
            msg = json.dumps(msg_aux._dict_)            

            #Envia a Mensagem, recebe a resposta e carrega o json da resposta
            self.cliente.send(msg.encode())
            resp_aux = self.cliente.recv(1024).decode()
            resp = json.loads(resp_aux)

            #Verifica a resposta e printa na tela
            if resp["requisicao"] == "PUT_OK":
                print (f"\nPUT_OK key: [{resp['chave']}] value: [{resp['valor']}] timestamp: [{resp['timestamp']}] realizada no servidor [{self.servidores[self.rngServ][0]}:{self.servidores[self.rngServ][1]}]")

                #Salva o dado na lista de chaves conhecidas pelo cliente
                dado = [resp['chave'], resp['timestamp']]
                self.chavesTS.append(dado)            
            else:
                print (f"\nPUT_ERROR key: [{resp['chave']}] value: [{resp['valor']}] timestamp: [{resp['timestamp']}] realizada no servidor [{self.servidores[self.rngServ][0]}:{self.servidores[self.rngServ][1]}]")

            #Desliga e Fecha o Socket
            self.cliente.shutdown(socket.SHUT_RDWR)
            self.cliente.close()

            #Atualiza o Timestamp
            self.clientTS = self.clientTS + 1
                        
        except socket.error as e: # Tratamento de erros
            print (f"erro {e}")

"""
Funcao GET: Executa a operação GET, enviando uma mensagem ao servidor com a chave e o timestamp associado a ela.
    Recebe uma resposta do servidor e atualiza a lista de timestamps das chaves.
"""
   def GET(self, key):
        try:            
            """
            Cria a mensagem padrao. Se a chave for conhecida, atualiza o valor do timestamp
            se nao, envia 0 mesmo
            """
            msg_aux = Mensagem("GET", key, "NULL", "0")            
            for item in self.chavesTS:
                if item[0] == key:
                    msg_aux = Mensagem("GET", item[0], "NULL", item[1])

            #Carrega e envia o JSON da mensagem
            msg = json.dumps(msg_aux._dict_)            
            self.cliente.send(msg.encode())

            #Recebe e carrega o JSON do resposta do servidor
            resp_aux = self.cliente.recv(1024).decode()
            resp = json.loads(resp_aux)

            #Verifica o tipo de resposta para printar na tela o resultado
            if resp["requisicao"] == "GET_ERROR":
                print ("\nTRY_OTHER_SERVER_OR_LATER")
            else: 
                print (f"\nGET key: [{key}] value: [{resp['valor']}] obtido do servidor [{self.servidores[self.rngServ][0]}:{self.servidores[self.rngServ][1]}], meu timestamp [{msg_aux.timestamp}] e do servidor [{resp['timestamp']}]")

                #Se a chave requisitada existir no servidor, salva a chave e o timestamp na lista de chaves conhecidas
                if resp["valor"] != "NULL":
                    dado = [resp['chave'], resp['timestamp']]
                    self.chavesTS.append(dado)

            #Desliga e fecha o socket
            self.cliente.shutdown(socket.SHUT_RDWR)
            self.cliente.close()
                        
        except socket.error as e: # Tratamento de erros
            print (f"erro na conexao erro {e}")

"""
Funcao menu: Mostra o menu de opções para o usuário.
"""
def menu():
    print ("\nSelecione uma opção (Digite um Numero):")
    print ("0. INIT do cliente")
    print ("1. PUT de chave")
    print ("2. GET de chave")
    print ("3. SAIR")

"""
Funcao Main: Função principal que inicia o cliente, solicita ao usuário o IP e a porta do usuario,
    e apresenta um menu com opções para o usuário escolher: INIT, PUT, GET ou SAIR.
"""
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
