#!/usr/bin/python
# -*- coding: utf-8 -*-
#monitoracao 04:51 as 19:59
import csv
import os, sys
import io, json
import re
from datetime import datetime,time
import cx_Oracle

#caminho_raiz='caminho completo'
caminho_raiz=''

#Funcao para criar a conexao com o banco de dados
def conectarOracle():
    try:
        con = cx_Oracle.connect('user','password','database')
    except cx_Oracle.Error as e:
        errorObj, = e.args
        escrever_log('connect_oracle',errorObj.message, errorObj.code)
    return con

#Funcao para realizar a consulta e retorno dos dados
def consultarOracle(con):
    cur = con.cursor()
    cur.execute("SELECT DISTINCT DATE_TIME, SOURCE_FILE, DEST_FILE, FILE_SIZE, REMOTE_NODE FROM DATABASE")
    retornoConsulta = cur.fetchall()
    escrever_log('connect_oracle','successful_connection', '200')
    return retornoConsulta

#Funcao para escrever dados no formato json valido
# def escrever_json(dados,nomeArquivo,gravacao):
#     with open(nomeArquivo,str(gravacao)) as f:
#         json.dump(dados,f,indent=4)

#Funcao para escrever dados no formato json sem identacao.
def escrever_json(dados,nomeArquivo,gravacao):
    with open(nomeArquivo,gravacao) as f:
        for x in dados:
            f.write('{}\n'.format(x).replace("'",'"'))
    nome_lista=nomeArquivo.split('/')
    escrever_log('write_export','export data file '+str(nome_lista[-1:]), '200')

#Função para registrar logs de execução do script
def escrever_log(stage,menssagem,status):

    #cabeçalho do texto: Timestamp|Menssagem|StatusCode
    menssagem = '{tempo},{stage},{menssagem},{status}\n'.format(
        tempo=str(datetime.now()),
        stage=str(stage),
        menssagem=str(menssagem),
        status=str(status)).replace(';','')

    with open(caminho_raiz+'logs/script_log.txt','a')as f:
        f.write(menssagem)

#Bloco principal
try:
    escrever_log('start_script','start of execution', '100')
    
    #Consulta e escrita dos dados do banco em lista
    con = conectarOracle()
    dadosConsultaBD = consultarOracle(con)

    #Carrega o TimeStamp da execução do Script
    #_hora_execucao = datetime.now()
    _hora_execucao = datetime.strptime('16:00','%H:%M')

    #lista com os arquivos enviados, obtidos na consulta ao Banco.
    lista_enviados = []
    _subLista_enviados = []
    _quantidade_arquivos_enviados = 0

    #lista com os arquivos nao enviados, obtidos na cunsulta fixa.
    lista_naoEnviados = []
    _subLista_naoEnviados = []
    _quantidade_arquivos_naoEnviados = 0

    #Abertura da tabela fixa em modo temporario
    #Descrição dos campos
        # [0]|CLIENTE    | CLIENT
        # [1]|DSNAME     | XXX.PX.X
        # [2]|REG        | 5
        # [3]|X          | 1
        # [4]|ROTINA     | SEMANAL 
        # [5]|TEMPO      | FIXO ou INTERVALO
        # [6]|T0         | 10:00
        # [7]|T1         | 10:10

    with open(caminho_raiz+'fixa_eventual.csv','r')as csvFixa:
        tabelaFixa = csv.reader(csvFixa)

        #Percorrendo dados da tabela fixa
        for _linha_fixa in tabelaFixa:
            
            #Carrega as variáveis com os dados para condição posterior
            _hora_inicial_fixa = datetime.strptime(_linha_fixa[6],'%H:%M')
            _hora_final_fixa = datetime.strptime(_linha_fixa[7],'%H:%M')
            _valida_tempo = _hora_execucao.time()>=_hora_inicial_fixa.time() and _hora_execucao.time()<=_hora_final_fixa.time()

            #Verifica se o registro corresponde ao periodo programado e aos dados fixos
            if(str(_linha_fixa[5])==('FIXO')) and (_valida_tempo==True):

                #verifica o numero de registros no banco de dados correspondente com o dsname da tabela fixa
                _contagem_dsname = len(re.findall(str(_linha_fixa[1]),str([_row_consulta_bd[1] for _row_consulta_bd in dadosConsultaBD])))
                _valida_quantidadade_enviada = False if _contagem_dsname != int(_linha_fixa[3]) else True

                for linha_bd_orac in dadosConsultaBD:
                    if len(re.findall(_linha_fixa[1],linha_bd_orac[1]))>0:
                        _subLista_enviados={
                            'TimeStamp':str(datetime.now()),
                            'Cliente':str(_linha_fixa[0]),
                            'Dsname':str(_linha_fixa[1]),
                            'ActionScript_Time':str(_hora_execucao.strftime('%H:%M')),
                            'Scheduled_Time':str(_hora_final_fixa.strftime('%H:%M')),
                            'Amount_Scheduled_Dsname_Hour':str(_linha_fixa[3]),
                            'Amount_Send_Dsname':str(_contagem_dsname),
                            'General_Quantity_Today':str(_linha_fixa[2]),
                            'Validator_Send_Scheduled': str(_valida_quantidadade_enviada),
                            'Date_Time':str(linha_bd_orac[0]),  #Avaliar
                            'Source_File':linha_bd_orac[1],     #Avaliar
                            'Dest_File':linha_bd_orac[2],       #Avaliar       
                            'File_Size':str(linha_bd_orac[3]),  #Avaliar
                            'Remote_Node':linha_bd_orac[4]      #Avaliar
                        }
                        lista_enviados.insert(len(lista_enviados),_subLista_enviados)
                        _quantidade_arquivos_enviados+=1

                #Verifica se existe diferença na programação de envio
                if _valida_quantidadade_enviada==False:

                    #Registra na sublista os dados não enviados
                    _subLista_naoEnviados={
                        #'TimeStamp':str(datetime.now()),
                        #'Dsname':str(_linha_fixa[1]),
                        #'Client_Dsname':str(_linha_fixa[0]),
                        'msg_alert':'{hora} - {cliente} - {dsname} - {enviado} de {programado}'.format(
                            hora=str(_hora_execucao.strftime('%H:%M')),
                            cliente=str(_linha_fixa[0]),
                            dsname=str(_linha_fixa[1]),
                            enviado=str(_contagem_dsname),
                            programado=str(_linha_fixa[3])
                        )
                    }
                    lista_naoEnviados.insert(len(lista_naoEnviados),_subLista_naoEnviados)
                    _quantidade_arquivos_naoEnviados+=1

        menssagem = 'General Quantity Send: {enviados} -- General Quantity Not Send: {naoEnviados}'.format(
            enviados=int(_quantidade_arquivos_enviados),
            naoEnviados=int(_quantidade_arquivos_naoEnviados)
        )
        escrever_log('process_data',str(menssagem), '150')

        #registra dados enviados no arquivo Json
        escrever_json(lista_enviados,caminho_raiz+'logs/export_send_list_eventual.json','a')

        #Registra dados não enviados no arquivo Json
        escrever_json(lista_naoEnviados,caminho_raiz+'logs/export_not_send_list_eventual.json','a')
        
except Exception as e:
    print("Falha na execucao do Script! Status erro: ",e)
finally:
    con.close()
    escrever_log('end_script','finally of execution', '300')
