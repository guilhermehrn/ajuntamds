import math
from dbtools import Dbtool
from similaridade import Similaridade
import concurrent.futures
from multiprocessing import Process
import os
import json


class JuncaoTools:
    """Classe que faz a juncao das tabelas do banco de dados."""

    def __init__(self, numtheads):
        self.similar = Similaridade()
        self.indexColCadUnic = {}
        self.tiposColCadUnic = {}
        self.indexColCnefe = {}
        self.tiposColCnefe = {}
        self.numInstancidadesCadUnicDic = {}
        self.codCidadesOrdenadaslist = []
        self.numTotalInstancias = 0
        self.numThread = numtheads
        self.tarefasParaTheads = []
        self.respostasThreads = []
        self.numInstanciasCadUnicPorThread = 1
        self.indexCadUnic = []
        self.indexCnefe = []

        self.formEndeCadunic = ["cod_familiar_fam",
                                "cod_munic_ibge_5_fam",
                                "nom_tit_logradouro_fam",
                                "nom_logradouro_fam",
                                "nom_localidade_fam",
                                "num_logradouro_fam",
                                "num_cep_logradouro_fam",
                                ]

        self.formEndeCnefe = ["cod_unico_endereco",
                              "nom_titulo_seglogr",
                              "nom_seglogr",
                              "dsc_localidade",
                              "num_endereco",
                              "cep"]

    def printarLista(self, lista):
        print("==>> Resutados finais :")
        for int in lista:
            print(int)

    def prepararDividirTarefa(self, nomeSchemaCadUnic, tabelaCadUnic, nomeScehemaCnefe, tabelaCnefe):

        with open("config.json", "r") as json_file:
            dadosConexao = json.load(json_file)

        bd = Dbtool(dadosConexao["host"], dadosConexao["port"], dadosConexao["base"], dadosConexao["user"],
                    dadosConexao["password"])

        print("Buscando os dados do cadastro unico no baco.")

        self.conjuntoCadUnic = bd.selecionarTabela(nomeSchemaCadUnic, [tabelaCadUnic], self.formEndeCadunic, '', 0)

        print("Buscando Indexadores ")
        print("Criando grupos de cidades")

        nometabelagrupoAux = tabelaCadUnic + "aux"

        bd.criarTabelaDeGrupos(nomeSchemaCadUnic, nometabelagrupoAux, tabelaCadUnic, "cod_munic_ibge_5_fam", 1)

        self.numInstancidadesCadUnicDic = dict(
            bd.selecionarTabela(nomeSchemaCadUnic, [nometabelagrupoAux], ["*"], '', 0))
        self.codCidadesOrdenadaslist = list(self.numInstancidadesCadUnicDic.keys())

        aux = 0

        for i in range(self.numThread):
            # self.respostasThreads.append([])
            self.tarefasParaTheads.append([])

        for row in self.conjuntoCadUnic:
            self.tarefasParaTheads[aux].append(row)
            aux += 1
            if aux == self.numThread:
                aux = 0

        self.conjuntoCadUnic = None

    def compararTabelas(self, nomeScehemaCnefe, tabelaCnefe, faixaCadUnico, nomeSchemaResult, nomeTabelaResult):

        cidadeCorrente = 0
        diceCoef = 0.0

        with open("config.json", "r") as json_file:
            dadosConexao = json.load(json_file)

        dbaseth = Dbtool(dadosConexao["host"], dadosConexao["port"], dadosConexao["base"], dadosConexao["user"],
                         dadosConexao["password"])

        for familia in self.tarefasParaTheads[faixaCadUnico]:
            if familia[2] != None:
                familia[2].replace(" ", "")

            if familia[3] != None:
                familia[3].replace(" ", "")

            if familia[4] != None:
                familia[4].replace(" ", "")

        for familia in self.tarefasParaTheads[faixaCadUnico]:

            if int(familia[1]) != cidadeCorrente:
                t = str(os.getpid())

                cidadeCorrente = int(familia[1])
                #print(t + ": buscando a Cidade " + str(cidadeCorrente))
                nometabelaCnefeCorrente = tabelaCnefe + "_" + str(cidadeCorrente)

                conjuntoCidadeCnefe = dbaseth.selecionarTabela(nomeScehemaCnefe, [nometabelaCnefeCorrente],
                                                               self.formEndeCnefe, '', 0)
                conjuntoCidadeCnefeDic = {}
                quantidadeConjCnefe = {}

                for linha in conjuntoCidadeCnefe:
                    preCepCnefe = str(int(linha[5]) // 1000)
                    # lprefcep = str(int(familia[self.indexColCadUnic["num_cep_logradouro_fam"]])/1000)

                    if preCepCnefe in conjuntoCidadeCnefeDic:
                        if linha[1] != None:
                            linha[1].replace(" ", "")

                        if linha[2] != None:
                            linha[2].replace(" ", "")

                        if linha[3] != None:
                            linha[3].replace(" ", "")
                        conjuntoCidadeCnefeDic[preCepCnefe].append(linha)
                        quantidadeConjCnefe[preCepCnefe] = quantidadeConjCnefe[preCepCnefe] + 1
                    else:
                        if linha[1] != None:
                            linha[1].replace(" ", "")

                        if linha[2] != None:
                            linha[2].replace(" ", "")

                        if linha[3] != None:
                            linha[3].replace(" ", "")

                        conjuntoCidadeCnefeDic[preCepCnefe] = [linha]
                        quantidadeConjCnefe[preCepCnefe] = 1

                conjuntoCidadeCnefe = None

                # print(t + ": comparando enderecos na cidade " + str(cidadeCorrente))

            enderecoCadUnic = [familia[2], familia[3], familia[4]]
            idfamilia = int(familia[0])
            resultadosPar = (idfamilia, 0, 0.0, 0)

            if familia[6] == None:
                preCepCad = str(int(0) // 1000)
            else:
                preCepCad = str(int(familia[6]) // 1000)
            numEnderCad = familia[5]

            i = 0
            diceCoef = 0.0

            if preCepCad in conjuntoCidadeCnefeDic:
                while not (math.isclose(diceCoef, 1.0) and numEnderCad == numEndCnefe) and i < quantidadeConjCnefe[preCepCad]:
                    # for endereco in self.conjuntoCidadeCnefeDic[preCepCad]:
                    endereco = conjuntoCidadeCnefeDic[preCepCad][i]
                    numEndCnefe = endereco[4]

                    # endcf = self.formEndeCnefe
                    enderecoCnefe = [endereco[1], endereco[2], endereco[3]]

                    idEndereco = int(endereco[0])

                    try:
                        diceCoef = self.similar.dice_coefficient1(','.join(enderecoCadUnic), ','.join(enderecoCnefe))
                        #diceCoef = self.similar.dice_coefficient1(','.join(enderecoCadUnic).replace(" ", ""), ','.join(enderecoCnefe).replace(" ", ""))
                    except(Exception):
                        mensagem = "Erro na comparação entre a instancia do Cad. Unico" + str(idfamilia) + "e o endereço do CNEFE " + str(idEndereco)
                        print(mensagem)

                    if diceCoef >= resultadosPar[2]:
                        if diceCoef >= 0.95 and numEnderCad == numEndCnefe and (numEnderCad not in [None, 0]) and (
                                numEndCnefe not in [None, 0]):
                            resultadosPar = (idfamilia, idEndereco, diceCoef, 5)

                        if diceCoef >= 0.95 and (numEnderCad in [None, 0]) and (numEndCnefe in [None, 0]):
                            resultadosPar = (idfamilia, idEndereco, diceCoef, 4)

                        if diceCoef >= 0.95 and numEnderCad != numEndCnefe and (numEnderCad not in [None, 0]) and (
                                numEndCnefe not in [None, 0]):
                            resultadosPar = (idfamilia, idEndereco, diceCoef, 3)

                        if diceCoef < 0.95 and numEnderCad == numEndCnefe and (numEnderCad not in [None, 0]) and (
                                numEndCnefe not in [None, 0]):
                            resultadosPar = (idfamilia, idEndereco, diceCoef, 2)

                        if diceCoef < 0.95 and (numEnderCad in [None, 0]) and (numEndCnefe in [None, 0]):
                            resultadosPar = (idfamilia, idEndereco, diceCoef, 1)

                        if diceCoef < 0.95 and numEnderCad != numEndCnefe and (numEnderCad not in [None, 0]) and (
                                numEndCnefe not in [None, 0]):
                            resultadosPar = (idfamilia, idEndereco, diceCoef, 0)

                    i = i + 1

            resp = [str(resultadosPar[0]), str(resultadosPar[1]), str(resultadosPar[2]), str(resultadosPar[3])]
            # self.respostasThreads[faixaCadUnico].append(resultadosPar)
            dbaseth.inseirdados(nomeSchemaResult, nomeTabelaResult, [resp])

    def armazenarRespostas(self, nomeSchema, tabelaResultado):

        with open("config.json", "r") as json_file:
            dadosConexao = json.load(json_file)

        bd = Dbtool(dadosConexao["host"], dadosConexao["port"], dadosConexao["base"], dadosConexao["user"],
                    dadosConexao["password"])
        for item in self.respostasThreads:
            bd.inseirdados(nomeSchema, tabelaResultado, item)

    def juntarTabelas(self, nomeSchemaCadUnic, tabelaCadUnic, nomeScehemaCnefe, tabelaCnefe, nomeTabelaResultado,
                      tipoResultado):

        with open("config.json", "r") as json_file:
            dadosConexao = json.load(json_file)

        bd = Dbtool(dadosConexao["host"], dadosConexao["port"], dadosConexao["base"], dadosConexao["user"],
                    dadosConexao["password"])

        nomeTabelaResult = nomeTabelaResultado
        listaAtributos = ['cod_familiar_fam numeric', 'cod_unico_endereco integer', 'dice_coeficiente double precision',
                          'nivel_precisao integer']

        if tipoResultado == 2:
            nomeTabelaResult = os.path.basename(nomeTabelaResultado).replace(".csv", "")
            bd.criartabela("public", nomeTabelaResult, listaAtributos, 1)

        if tipoResultado == 0:
            bd.criartabela("public", nomeTabelaResult, listaAtributos, 1)

        self.prepararDividirTarefa(nomeSchemaCadUnic, tabelaCadUnic, nomeScehemaCnefe, tabelaCnefe)

        print("Criando processos\n")

        threads = []
        # executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.numThread, thread_name_prefix="Minion")
        for i in range(self.numThread):
            threads.append(Process(target=self.compararTabelas, name="Ajuntamds",
                                   args=(nomeScehemaCnefe, tabelaCnefe, i, "public", nomeTabelaResult)))

        for i in range(self.numThread):
            threads[i].start()
            # threads[i].join()

        print("Processando Daddos...")

        for i in range(self.numThread):
            threads[i].join()
            # self.compararTabelas(nomeScehemaCnefe,tabelaCnefe, i,"public", nomeTabelaResult)
            # print(threads[i])

        if tipoResultado == 2:
            f = open(nomeTabelaResultado, "w")
            f.write(','.join(['cod_familiar_fam', 'cod_unico_endereco', 'dice_coeficiente', 'nivel_precisao']) + '\n')
            f.close()
            f = open(nomeTabelaResultado, "a")

            resp = bd.selecionarTabela("public", [nomeTabelaResult], ["*"], '', 0)

            for row in resp:
                f.write(str(row[0]) + ',' + str(row[1]) + ',' + str(row[2]) + ',' + str(row[3]) + '\n')

            f.close()

if __name__ == '__main__':
    print("a")