from juncaoTools import JuncaoTools
from preProcessamento import PreProcessamento

import time
import sys
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Argumentos do Ajunta mds', argument_default=1)


    parser.add_argument('--cadunic', "-ca", required=True, metavar='nome_esquema.nome_tabela',
                        help="Entrada das tabelas de dados do Cadastro Unico.")

    parser.add_argument('--cnefe', "-cn", required=True, metavar='nome_esquema.nome_tabela',
                        help="Entrada das tabelas de dados do CNEFE")

    parser.add_argument('--threads', "-t", required=False, metavar='numero_threads', type=int, default=4,
                        help="Número de Thread que o programa irá ultilizar para processar os dados.")

    parser.add_argument('--preprocess', "-p", required=False, metavar='', type=int, default=0,
                        help="Força o pre-processamento dos dados.")

    parser.add_argument('--saida', "-s", required=False, metavar='nome_tabela',
                        help = "Saida padrão do programa. A tabela resultado será gravada no proprio banco de dados dentro do esquema resultados.")

    parser.add_argument('--saidacsv', "-sc", required=False, metavar='nome_arquivo.csv',
                        help="A saída do programa sera em formato em arquivo csv. A tabela resultado será gravada no proprio banco de dados dentro do esquema resultados.")

    parser.add_argument('--saidamult', "-sm", required=False, metavar='nome_arquivo.csv',
                        help="Multiplas saídas. O programa irá gerar a tabela de resultados dentro do banco de dados quanto em formato csv.")

    args = parser.parse_args()

    j = JuncaoTools(args.threads)

    prep = PreProcessamento()

    cadunicoEsquema = str(args.cadunic).split(".")[0]
    cadunicoTabela = str(args.cadunic).split(".")[1]

    cnefeEsquema = str(args.cnefe).split(".")[0]
    cnefeTabela = str(args.cnefe).split(".")[1]

    if args.preprocess == 1:
        prep.testarseProcessado(cadunicoEsquema, cadunicoTabela)
        prep.preProcessarBaseCnefe(cnefeEsquema, cnefeTabela)


    ini = time.time()
    #j.juntarTabelas(cadunicoEsquema, cadunicoTabela, cnefeEsquema, cnefeTabela)
    #j.juntarTabelas("cad_unic_2019", "rr10000", "cnefe_rr_14", "14_rr")

    ##Por enquanto estou chamando as funções manualmente pois o programa ainda está em faze de teste.
    #j.juntarTabelas("cad_unic_2019", "base_cad_unic_2019_35", "cnefe_sp_35", "35_sp")
    fim = time.time()
    print("\n=========================================\n")
    print("Tempo: " + str(fim - ini))