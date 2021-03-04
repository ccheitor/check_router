#!/usr/bin/python
# -*- coding: utf-8 -*-
#monitoracao 05:00 as 20:00
import csv
import os, sys
import io, json
import re
import datetime
import cx_Oracle

#caminho_raiz='/etc/caminho completo/'
caminho_raiz=''

#Funcao para criar a conexao com o banco de dados
def conectarOracle():
    try:
        con = cx_Oracle.connect('user','password','database')
    except Exception as e:
        print("Falha na conexao com o banco de dados! Status erro: ",e)
    return con

#Funcao para realizar a consulta e retorno dos dados
def consultarOracle(con):
    cur = con.cursor()
    cur.execute("SELECT DATE_TIME, SOURCE_FILE, DEST_FILE, FILE_SIZE, REMOTE_NODE FROM DATABASE")
    retornoConsulta = cur.fetchall()
    return retornoConsulta

# #Funcao para escrever dados no formato json identado
# def escrever_json(dados,nomeArquivo):
#     with io.open(nomeArquivo,'wb') as f:
#         json.dump(dados,f,ensure_ascii=False,indent=4)

#Funcao para escrever dados no formato json sem identacao.
def escrever_json(dados,nomeArquivo,gravacao):
    with open(nomeArquivo,gravacao) as f:
        for x in dados:
            f.write('{}\n'.format(x).replace("'",'"'))

#Bloco principal
try:

    #Consulta e escrita dos dados do banco em lista
    con = conectarOracle()
    dadosConsultaBD = consultarOracle(con)

    #lista com os arquivos enviados, obtidos na consulta ao Banco.
    lista_dados_enviados = []
    subLista_enviados = []

    #lista com os arquivos nao enviados, obtidos na cunsulta fixa.
    lista_dados_naoEnviados = []
    subLista_naoEnviados = []

    #Percorrendo dados da consulta para prenchimento do Json
    #Primeiro passo e verificar todos os dados que estao nos dados do banco mas nao estao na fixa
    for linhaConsulta in dadosConsultaBD:

        #inicializacao das variveis vazias
        validadorRegex=0
        quantidadeRegistrosEsperados = 0
        quantidadeRegistrosEnviados = 0
        nomeClienteDsname =''
        nomeDsname=''
        validadorCadastro = False

        #Abertura da tabela fixa em modo temporario
        with open(caminho_raiz+'fixa.csv','r')as csvFixa:
            tabelaFixa = csv.reader(csvFixa)

            #Percorrendo dados da tabela fixa
            for linhaFixa in tabelaFixa:

                #busca a referencia do dnsame na fixa vs consulta
                validadorRegex = len(re.findall(linhaFixa[1],linhaConsulta[1]))

                #Se encontrar o DSNAME, carrega os dados nas variaveis
                if validadorRegex>0:
                    quantidadeRegistrosEsperados=linhaFixa[2]
                    nomeClienteDsname=linhaFixa[0]
                    nomeDsname = linhaFixa[1]
                    quantidadeRegistrosEnviados= [nomeDsname in x for x in [y[1] for y in dadosConsultaBD]].count(True)
                    validadorCadastro = True
                    break
                else:
                    nomeClienteDsname ='Nao Cadastrado'
                    nomeDsname='Nao Cadastrado'
                    validadorCadastro = False

        #Carrega dados do json
        subLista_enviados={
            'Date_Time':str(linhaConsulta[0]),
            'Source_File':linhaConsulta[1],
            'Dest_File':linhaConsulta[2],
            'File_Size':str(linhaConsulta[3]),
            'Remote_Node':linhaConsulta[4],
            'TimeStamp':str(datetime.datetime.now()),
            'Dsname':nomeDsname,
            'Client_Dsname':nomeClienteDsname,
            'Amount_Scheduled_Dsname':str(quantidadeRegistrosEsperados),
            'Amount_Send_Dsname':str(quantidadeRegistrosEnviados),
            'Validator_Send_Scheduled': str(False if int(quantidadeRegistrosEsperados)!= int(quantidadeRegistrosEnviados) else True),
            'Validator_Register': str(validadorCadastro)
        }
        lista_dados_enviados.insert(len(lista_dados_enviados),subLista_enviados)

    #logTemp = datetime.datetime.now()
    escrever_json(lista_dados_enviados,caminho_raiz+'logs/export_send_list.json','a')

    with open(caminho_raiz+'fixa.csv','r')as csvFixa:
        tabelaFixa = csv.reader(csvFixa,delimiter=',',skipinitialspace=False)

        for linhaFixa in tabelaFixa:

            validadorRegex = len(re.findall(str(linhaFixa[1]),str([x[1] for x in dadosConsultaBD])))

            if validadorRegex<=0 or int(linhaFixa[2])!=validadorRegex:
                subLista_naoEnviados={
                    #'TimeStamp':str(datetime.datetime.now()),
                    #'Dsname':linhaFixa[1],
                    #'Client_Dsname':linhaFixa[0],
                    'msg_alert':(linhaFixa[0]+' - '+linhaFixa[1]+' - Enviado - '+str(validadorRegex)+' de '+str(linhaFixa[2]))
                }
                lista_dados_naoEnviados.insert(len(lista_dados_naoEnviados),subLista_naoEnviados)

    escrever_json(lista_dados_naoEnviados,caminho_raiz+'logs/export_not_send_list.json','wb')

except Exception as e:
    print("Falha na execucao do Script! Status erro: ",e)
finally:
    con.close()
#    print('Fim da execucao')
