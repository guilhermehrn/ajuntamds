from juncaoTools import JuncaoTools
from preProcessamento import PreProcessamento

import time
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Argumentos do Ajunta mds', argument_default=1)

    parser.add_argument('--cadunic', "-ca", required=True, metavar='nome_esquema.nome_tabela',
                        help="Entrada das tabelas de dados do Cadastro Unico.")

    parser.add_argument('--cnefe', "-cn", required=True, metavar='nome_esquema.nome_tabela',
                        help="Entrada das tabelas de dados do CNEFE")

    parser.add_argument('--threads', "-t", required=False, metavar='numero_threads', type=int, default=4,
                        help="Numero de Thread que o programa ira ultilizar para processar os dados.")

    parser.add_argument('--preprocess', "-p", required=False, metavar='', nargs='?', default=0, const=1,
                        help="Forca o pre-processamento dos dados.")

    parser.add_argument('--saida', "-s", required=False, metavar='nome_tabela', nargs='?', default=0, const=1,
                        help="Saida padrao do programa. A tabela resultado sera gravada no proprio banco de dados dentro do esquema resultados.")

    parser.add_argument('--saidacsv', "-sc", required=False, metavar='nome_arquivo.csv', nargs='?', default=0,
                        help="A saida do programa sera em formato em arquivo csv. A tabela resultado sera gravada no proprio banco de dados dentro do esquema resultados.")

    parser.add_argument('--saidamult', "-sm", required=False, metavar='nome_arquivo.csv', nargs='?', default=0, const=1,
                        help="Multiplas saidas. O programa ira gerar a tabela de resultados dentro do banco de dados quanto em formato csv.")

    args = parser.parse_args()

    j = JuncaoTools(args.threads)

    prep = PreProcessamento()

    cnefeEsquema = str(args.cnefe).split(".")[0]
    cnefeTabela = str(args.cnefe).split(".")[1]

    cadunicoEsquema = str(args.cadunic).split(".")[0]
    cadunicoTabela = str(args.cadunic).split(".")[1]
    cadunicoTabelaUf = str(args.cadunic).split(".")[1] + "_" + cnefeTabela.split("_")[0]



    nomeTabelaResultado = ""

    print("AJUNTA MDS\n")
    print("Programa de correlacao de bases do CNEFE com as bases do Cadastro Unico\n")
    print("=========================================================================\n")

    # print(args.preprocess)
    ini = time.time()
    if args.preprocess == 1:
        print("Preprocessamento obrigatorio ativo ")
        print("preprocessando Tabela do Cadastro Unico.")
        prep.testarseProcessado(cadunicoEsquema, cadunicoTabela)
        prep.preProcessarBaseCadUnico(cadunicoEsquema, cadunicoTabela)
        print("Preprocessamento do Cadastro Unico concluido.\n")

        print("Preprocessando Tabela do CNEFE.")
        prep.testarseProcessado(cnefeEsquema, cnefeTabela)
        prep.preProcessarBaseCnefe(cnefeEsquema, cnefeTabela)
        print("Preprocessamento do CNEFE concluido.\n")

    if not prep.testarseProcessado(cadunicoEsquema, cadunicoTabela):
        print("preprocessando Tabela do Cadastro Unico que nao havia sido processada.")
        prep.preProcessarBaseCadUnico(cadunicoEsquema, cadunicoTabela)
        print("Preprocessamento do Cadastro Unico concluido.\n")

    if not prep.testarseProcessado(cnefeEsquema, cnefeTabela):
        print("preprocessando Tabela do CNEFE que nao havia sido processada.")
        prep.preProcessarBaseCnefe(cnefeEsquema, cnefeTabela)
        print("Preprocessamento concluido.\n")

    if args.saida == 1 and args.saidacsv == 0 and args.saidamult == 0:
        nomeTabelaResultado = cnefeTabela +'_'+cadunicoTabela

        # tipoResultado: 0 se saida padrao, dentro do banco
        # 1: se for saida somente em formato CSV
        # 2: se for resultado em tabela no banco e em formato csv
        j.juntarTabelas(cadunicoEsquema, cadunicoTabelaUf, cnefeEsquema, cnefeTabela, nomeTabelaResultado, 0)

    if args.saida != 0 and args.saida != 1 and args.saidacsv == 0 and args.saidamult == 0:
        nomeTabelaResultado = args.saida
        j.juntarTabelas(cadunicoEsquema, cadunicoTabelaUf, cnefeEsquema, cnefeTabela, nomeTabelaResultado, 0)

    if args.saida == 0 and args.saidacsv != 0 and args.saidamult == 0:
        # pre processando o pach.

        nomeTabelaResultado = str(args.saidacsv).replace("\\", "/")
        #print(nomeTabelaResultado)
        j.juntarTabelas(cadunicoEsquema, cadunicoTabelaUf, cnefeEsquema, cnefeTabela, nomeTabelaResultado, 1)

    if args.saida == 0 and args.saidacsv == 0 and args.saidamult != 0:
        # pre processando o pach.

        nomeTabelaResultado = str(args.saidamult).replace("\\", "/")
        print(nomeTabelaResultado)

        j.juntarTabelas(cadunicoEsquema, cadunicoTabelaUf, cnefeEsquema, cnefeTabela, nomeTabelaResultado, 2)

    # j.juntarTabelas(cadunicoEsquema, cadunicoTabela, cnefeEsquema, cnefeTabela)
    # nomeTabelaResult = "resultado_" + nomeSchemaCadUnic.split("_")[2] + "_" + tabelaCnefe
    # j.juntarTabelas("cad_unic_2019", "rr10000", "cnefe_rr_14", "14_rr")
    # j.juntarTabelas("cad_unic_2019", "base_cad_unic_2019_35", "cnefe_sp_35", "35_sp")
    fim = time.time()
    print("\n=========================================================================\n")
    print("Tempo de processemento: " + str(fim - ini))
