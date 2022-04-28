# -*- coding: utf-8 -*-
from modules.postgres import Conector_postgres
import pandas as pd
from tqdm import tqdm #se não tiver essa lib, favor instalar para não bugar comando: ("pip install tqdm")
from time import sleep
from os import system
'''
Função criada para leitura e tratamento de arquivos csv, contendo uma barra de progresso que informa o usuário,
quantos registros ele irá inserir e o tempo restante para a conclusão da inserção no banco de dados, 
trazendo os dados inseridos na tabela.

By: Ricardo Alex de Lima

'''
system('CLS')

if __name__ =="__main__":

    try:
        def leitura_e_tratamento():
            #trazendo o conector
            banco = Conector_postgres(host="127.0.0.1", db="ATV14")
            
            #adicione o caminho correto para a leitura do arquivo no formato .csv
            df = pd.read_csv(input('Digite o caminho do arquivo csv: '))
            print('\n')
            
            #tratando todos os campos em branco
            dados = df.dropna(how="any", axis=0)
            
            #trazendo o resultado do tratamento no terminal, e o total de dados contidos no arquivo pós tratamento
            print(f'Seu arquivo foi tratado, e possui um total de', dados.shape,'linhas e colunas.\n' )
            sleep(4)
            
            print('iniciando a inserção dos dados, por favor aguarde...')
            sleep(2)
            
            #adicionando a barra de progresso que mostra a quantidade de registros, pegando pelo índice, 
            # junto com tempo que falta para a conclusão da inserção.
            with tqdm(total=df.shape[0]) as pbar: 
                #percorrendo o csv tratado e inserindo no banco
                for index, row in dados.iterrows():
                    pbar.update(1)
                    banco.inserir(f"INSERT INTO aula_csv (datas, valor) VALUES ('{row['data']}', '{row['valor']}'); ")   
                    sleep(0.25)
                #retorno do for
                print('Os registros inseridos com sucesso!\n')
                print('\n')
            #fazendo uma nova query
            df = pd.DataFrame(banco.selecionar(f"SELECT * FROM aula_csv"))
            #retorno da query
            print(df)
        #fim da função
        leitura_e_tratamento()
        
    except Exception as e :
        print(str(e))
        
        Copyright © Company Name. All Rights Reserved.
