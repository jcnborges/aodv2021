import pandas as pd
import networkx as nx
import os
import csv
import json
import sys
import threading
import time
import keyboard
from os import listdir
from os.path import isfile, join
from numpy import mean
from numpy import std

# ==========================================================    
# Declaração de constantes
# ==========================================================  
CSV_FILE = "../csv/pacotes_e2e_medias.csv"
CSV_DIR = "../csv/metricas_redes_complexas/"
JSON_FILE = "lista_arquivos.json"
DIR = '../rt2/'
DIC_SPEED = {
    "Static": 0,
    "IPv4SlowMobility": 1,
    "IPv4ModerateFastMobility": 2,
    "IPv4FastMobility": 3,
    "IPv4Fast4Mobility": 4,
    "IPv4Fast5Mobility": 5,
    "IPv4Fast6Mobility": 6,
}
NAO_PROCESSADO = "Não processado"
PROCESSANDO = "Processando"
PROCESSADO = "Processado"
ERRO = "Erro"
WAIT_TIME = 1

# ==========================================================    
# Declaração de variáveis globais
# ========================================================== 
g_lista_arquivos = []

# ==========================================================    
# Varre todos os subdiretórios da raiz procurando 
# as tabelas de roteamento (arquivos .rt)
#
# Parâmetros:
#   raiz - diretório raiz das tabelas de roteamento
#
# Saída:
#   Lista de arquivos carregada
#   False - lista de arquivos não carregada
# ==========================================================    
def ler_lista_arquivos(raiz):
    global g_lista_arquivos
    if (not os.path.exists(CSV_FILE)):
        print("É necessário existir o arquivo CSV dos cenários!")
        return False
 
    df_cenarios = pd.read_csv(CSV_FILE, low_memory = False)

    # Verifica se o arquivo json contendo a lista de arquivos .rt existe
    # caso não, varre o diretório raiz em busca dos arquivos e monta
    # nova lista
    if (os.path.exists(JSON_FILE)):
        with open(JSON_FILE) as json_file:            
            g_lista_arquivos = json.load(json_file)
        return True

    for d in os.listdir(raiz):
        for f in os.listdir(join(DIR, d)):
            arq = join(DIR, d, f)
            dic_arquivo = {
                "scenario": d,
                "arquivo_rt": arq,
                "arquivo_csv": join(CSV_DIR, "{0}_{1}.csv".format(d, f.replace(".rt", ""))),
                "situacao": NAO_PROCESSADO,
                "detalhes": None
            }
            scenario = dic_arquivo["scenario"]
            info = scenario.split("_")
            speed = DIC_SPEED[info[0]]
            info_hosts = info[1]
            timeout = info[2]
            round_idx = dic_arquivo["arquivo_rt"].split("-")[1].replace(".rt", "")
            print('speed == {0} and hosts == {1} and timeout = {2} and round_idx == {3}'.format(speed, info_hosts, timeout, round_idx))
            if (df_cenarios.query('speed == {0} and hosts == {1} and timeout == {2} and round_idx == {3}'.format(speed, info_hosts, timeout, round_idx), engine = 'python').empty):
                continue
            g_lista_arquivos.append(dic_arquivo)
    return True

# ==========================================================    
# Grava a lista de arquivos atualizada
#
# Parâmetros:
#   destino - caminho do arquivo de saída .json
#
# Saída:
#   True - arquivo gravado com sucesso
#   False - arquivo não gravado
# ========================================================== 
def gravar_lista_arquivos(destino):
    global g_lista_arquivos
    with open(destino, 'w') as outfile:        
        outfile.write(json.dumps(g_lista_arquivos, ensure_ascii = False))

# ==========================================================
# Processa uma tabela de roteamento (arquivo .rt) 
# passado como parâmetro de entrada
# 
# Parâmetros:
#   dic_arquivo - dicionário do arquivo
# ==========================================================
def processar_tabela_roteamento(dic_arquivo):
    try:
        # ==========================================================
        # Lê as informações do cenário e rodada
        # ==========================================================
        scenario = dic_arquivo["scenario"]
        info = scenario.split("_")
        speed = DIC_SPEED[info[0]]
        info_hosts = info[1]
        timeout = info[2]
        round_idx = dic_arquivo["arquivo_rt"].split("-")[1].replace(".rt", "")

        # ==========================================================
        # Limpa caracteres impróprios dos arquivo .rt
        # ==========================================================

        # Read in the file
        with open(dic_arquivo["arquivo_rt"], 'r') as file :
          filedata = file.read()

        # Replace the target string
        filedata = filedata.replace('  ', ' ')
        filedata = filedata.replace('/', ' ')
        filedata = filedata.replace('s', '')
        filedata = filedata.replace('#', '')

        # Write the file out again
        with open(dic_arquivo["arquivo_rt"], 'w') as file:
          file.write(filedata)
        
        # ==========================================================
        # Cria um dataframe Pandas para receber as métricas de cada
        # snapshot
        # ==========================================================
        df_metricas = pd.DataFrame(columns = ["scenario", "speed", "hosts", "timeout", "round_idx", "snapshot", "avg_degree", "std_degree", "density", "avg_clustering_coef",  "avg_shortest_path", "diameter", "arquivo"])

        # ==========================================================
        # Lê o arquivo como um dataframe Pandas
        # ==========================================================
        columns = ['acao', 'evento', 'timestamp', 'tabela', 'origem', 'destino', 'mascara', 'gateway', 'interface']
        aodv = pd.read_csv(dic_arquivo["arquivo_rt"], names = columns, sep = ' ')
        hosts = aodv[aodv.destino == '127.0.0.0']['origem']
        eventos = aodv[aodv.timestamp != 0][['acao', 'timestamp', 'origem', 'destino', 'gateway']]

        # ==========================================================
        # Processa a tabela de rotamento com variação no tempo
        # ==========================================================
        start = 0 # início da simulação
        stop = 200 # condição de parada em segundos
        snapshot = 1000 # intervalo de amostragem do snapshot (em milisegundos)

        t = start
        while t <= stop:
            rotas = []
            for idx, e in eventos[eventos.timestamp <= t].iterrows():
                cria = False
                rota = next((r for r in rotas if r['origem'] == e['origem'] and r['destino'] == e['destino']), None)
                if rota == None:
                    cria = True
                    rota = {'origem': e['origem'], 'destino': e['destino'], 'gateway': e['gateway']}
                if '+' in e['acao'] and cria: # criação de nova rota
                    rotas.append(rota)
                elif '*' in e['acao']: # alteração de rota existente
                    rota.update({'gateway': e['gateway']})
                else: # removação de rota existente
                    rotas.remove(rota)

            # construção do grafo
            G = nx.DiGraph()
            c = 1
            for h in hosts:
                G.add_node(h, **{'c':c})
                c += 1
            for r in rotas:
                G.add_edge(r['origem'], r['gateway'])

            # ==========================================================
            # Cálculo das métricas e geração do dataframe Pandas
            # ==========================================================
            try:
                sp = nx.average_shortest_path_length(G)
            except:
                sp = None
            try:
                diam = nx.diameter(G)
            except:
                diam = None
            dg = []
            for n, degree in nx.degree(G):
                dg.append(degree)

            data = pd.DataFrame(
                {"scenario": scenario,
                 "speed": pd.to_numeric(speed),
                 "hosts": pd.to_numeric(info_hosts),
                 "timeout": pd.to_numeric(timeout),
                 "round_idx": pd.to_numeric(round_idx),
                 "snapshot": t, 
                 "avg_degree": mean(dg), 
                 "std_degree": std(dg),
                 "density": nx.density(G), 
                 "avg_clustering_coef": nx.average_clustering(G),
                 "avg_shortest_path": sp,
                 "diameter": diam,
                 "arquivo": dic_arquivo["arquivo_rt"]
                },
                index=[0]
            )
            df_metricas = df_metricas.append(data, ignore_index = True)
            t += snapshot / 1000

        # Salva dataframe em CSV
        df_metricas.to_csv(dic_arquivo["arquivo_csv"])
        
        # Atualiza status do arquivo
        dic_arquivo.update({"situacao": PROCESSADO})
        return True
    except Exception as e:
        dic_arquivo.update({"situacao": ERRO})
        dic_arquivo.update({"detalhes": repr(e)})
        print(repr(e))
        return False

# ==========================================================    
# Mostra o status do processamento
# ==========================================================        
def mostrar_status_processamento():
    nao_processado = 0
    processado = 0
    processando = 0
    erro = 0
    total = 0
    for dic_arquivo in g_lista_arquivos:
        if (dic_arquivo["situacao"] == PROCESSADO):
            processado += 1
        elif (dic_arquivo["situacao"] == PROCESSANDO):
            processando += 1
        elif (dic_arquivo["situacao"] == NAO_PROCESSADO):
            nao_processado += 1
        else:
            erro += 1
        total += 1
    os.system("clear")
    print("---------------------------------------")
    print("TOTAL DE ARQUIVOS .RT    :   {0}".format(total))
    print("---------------------------------------")
    print(" > Processados           :   {0}".format(processado))
    print(" > Processando           :   {0}".format(processando))
    print(" > Não processados       :   {0}".format(nao_processado))
    print(" > Erro                  :   {0}".format(erro))
    print("---------------------------------------")
    print("PROGRESSO                :   {0:.2f}%".format(100 * processado / total))
    print("---------------------------------------")

# =========================================================
# Verifica se todos os arquivos foram processados
# retorna verdadeiro caso sim
# =========================================================
def verificar_encerramento():
    processado = 0
    erro = 0
    total = 0
    for dic_arquivo in g_lista_arquivos:
        if (dic_arquivo["situacao"] == PROCESSADO):
            processado += 1
        elif (dic_arquivo["situacao"] == ERRO):
            erro += 1
        total += 1
    return processado == total #- erro

# ==========================================================
# Código principal das threads, processa todos os arquivos
# de roteamento encontrados
# ==========================================================
def processar_arquivos_roteamento():
    while True:
        # Verifica se todos os arquivos foram processados e
        # encerra a Thread caso sim
        if verificar_encerramento():
            return

        # Varre a lista de arquivos em busca de um que não tenha sido
        # processado ou esteja sendo processado ou não tenha erro
        for dic_arquivo in g_lista_arquivos:
            if (not dic_arquivo["situacao"] in [PROCESSADO, PROCESSANDO]):
                break

        dic_arquivo.update({"detalhes": None})
        # Verifica se o arquivos CSV já existe caso o JSON de controle tenha sido apagado
        if (os.path.exists(dic_arquivo["arquivo_csv"])):
            dic_arquivo.update({"situacao": PROCESSADO})
        else:
            dic_arquivo.update({"situacao": PROCESSANDO})
            processar_tabela_roteamento(dic_arquivo)

        gravar_lista_arquivos(JSON_FILE)

# ==========================================================    
# Implementação do script principal
# ========================================================== 
def main(args):
    # Verifica se o diretório CSV de saída existe e cria, caso não exista
    if (not os.path.exists(CSV_DIR)):
        os.mkdir(CSV_DIR)
    if (len(args) == 1):
        n_threads = 1
    else:
        n_threads = int(args[1])
    try:
        if (not ler_lista_arquivos(DIR)):
            print("Não foi possível ler a lista de arquivos!")
            os._exit(0)
        # Transforma todos os arquivos que foram processados
        # de forma incompleta e reinicia como NAO_PROCESSADO
        for dic_arquivo in g_lista_arquivos:
            if (dic_arquivo["situacao"] == PROCESSANDO):
                dic_arquivo.update({"situacao": NAO_PROCESSADO})
        for i in range(n_threads):
            print("Iniciando thread {0}...".format(i))
            threading.Thread(
                target = processar_arquivos_roteamento, 
                args = ()
            ).start()
        while (not verificar_encerramento()):
            mostrar_status_processamento()
            print("Segure ESC para sair")
            if keyboard.is_pressed('Esc'):
                os._exit(0)
            time.sleep(WAIT_TIME)
    except Exception as e:
        print(str(e))
    finally:
        print("Programa encerrado.")
        gravar_lista_arquivos(JSON_FILE)
    return 0
 
if __name__ == '__main__':
    sys.exit(main(sys.argv))
