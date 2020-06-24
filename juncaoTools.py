import math
import time

from dbtools import Dbtool
from similaridade import Similaridade
import concurrent.futures
from multiprocessing import Process
import os
from concurrent.futures import ALL_COMPLETED
import threading


# noinspection DuplicatedCode
class JuncaoTools:
    def __init__(self, numtheads):
        # self.bd = Dbtool("localhost", "5432", "mds_cad_unic", "postgres", "2631")
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
        # "cod_munic_ibge_5_fam","cod_munic_ibge_2_fam","des_complemento_fam"

        self.formEndeCnefe = ["cod_unico_endereco",
                              "nom_titulo_seglogr",
                              "nom_seglogr",
                              "dsc_localidade",
                              "num_endereco",
                              "cep"]

        # "dsc_modificador","cod_municipio","cod_uf"

    def printarLista(self, lista):
        print("==>> Resutados finais :")
        for int in lista:
            print(int)

    def prepararDividirTarefa(self, nomeSchemaCadUnic, tabelaCadUnic, nomeScehemaCnefe, tabelaCnefe):

        bd = Dbtool("localhost", "5432", "mds_cad_unic", "postgres", "2631")
        print("Buscando os dados do  cad unico no baco.")

        self.conjuntoCadUnic = bd.selecionarTabela(nomeSchemaCadUnic, [tabelaCadUnic], self.formEndeCadunic, '', 0)

        print("Buscando Indexadores.")

        #self.indexColCadUnic = bd.retornarColunasIndex(nomeSchemaCadUnic, tabelaCadUnic)
        #self.tiposColCadUnic = bd.retornarColunasTypes(nomeSchemaCadUnic, tabelaCadUnic)

        #self.indexColCnefe = bd.retornarColunasIndex(nomeScehemaCnefe, tabelaCnefe)
        #self.tiposColCnefe = bd.retornarColunasTypes(nomeScehemaCnefe, tabelaCnefe)

        print("Criando grupos de cidades.")
        nometabelagrupoAux = tabelaCadUnic + "aux"

        bd.criarTabelaDeGrupos(nomeSchemaCadUnic, nometabelagrupoAux, tabelaCadUnic, "cod_munic_ibge_5_fam", 1)

        self.numInstancidadesCadUnicDic = dict(
            bd.selecionarTabela(nomeSchemaCadUnic, [nometabelagrupoAux], ["*"], '', 0))
        self.codCidadesOrdenadaslist = list(self.numInstancidadesCadUnicDic.keys())

        #self.numTotalInstancias = bd.contarInstancias(nomeSchemaCadUnic, tabelaCadUnic)

        #self.numInstanciasCadUnicPorThread = self.numTotalInstancias // self.numThread
        aux = 0

        for i in range(self.numThread):
            #self.respostasThreads.append([])
            self.tarefasParaTheads.append([])

        for row in self.conjuntoCadUnic:
            self.tarefasParaTheads[aux].append(row)
            aux += 1
            if aux == self.numThread:
                aux = 0

        self.conjuntoCadUnic = None
    # def criarMascaraEnderecos(self):
    #
    #     for i in range(len(self.mascaraEnderecoCadUnic)):
    #         cad = self.mascaraEnderecoCadUnic[i]
    #         cenef = self.mascaraEnderecoCnefe[i]
    #         self.indexCadUnic.append(self.indexColCadUnic[cad])
    #         self.indexCnefe.append(self.indexColCnefe[cenef])

    def compararTabelas(self, nomeScehemaCnefe, tabelaCnefe, faixaCadUnico, nomeSchemaResult, nomeTabelaResult):

        cidadeCorrente = 0

        # conjuntoCidadeCnefe = []
        # resultadosPar = (1, 1, 0.0)
        diceCoef = 0.0

        dbaseth = Dbtool("localhost", "5432", "mds_cad_unic", "postgres", "2631")
        print("MEU PAI E: ", os.getppid())

        for familia in self.tarefasParaTheads[faixaCadUnico]:

            if int(familia[1]) != cidadeCorrente:
                t = str(os.getpid())

                cidadeCorrente = int(familia[1])
                # print(t + ": buscando a Cidade " + str(cidadeCorrente))
                nometabelaCnefeCorrente = tabelaCnefe + "_" + str(cidadeCorrente)

                conjuntoCidadeCnefe = dbaseth.selecionarTabela(nomeScehemaCnefe, [nometabelaCnefeCorrente], self.formEndeCnefe, '', 0)
                conjuntoCidadeCnefeDic = {}
                quantidadeConjCnefe = {}

                # print(t + ": montando arvore para cidade " + str(cidadeCorrente))

                for linha in conjuntoCidadeCnefe:
                    preCepCnefe = str(int(linha[5]) // 1000)
                    # lprefcep = str(int(familia[self.indexColCadUnic["num_cep_logradouro_fam"]])/1000)

                    if preCepCnefe in conjuntoCidadeCnefeDic:
                        conjuntoCidadeCnefeDic[preCepCnefe].append(linha)
                        quantidadeConjCnefe[preCepCnefe] = quantidadeConjCnefe[preCepCnefe] + 1
                    else:
                        conjuntoCidadeCnefeDic[preCepCnefe] = [linha]
                        quantidadeConjCnefe[preCepCnefe] = 1

                conjuntoCidadeCnefe = None

                # print(t + ": comparando enderecos na cidade " + str(cidadeCorrente))

            enderecoCadUnic = [familia[2], familia[3], familia[4]]
            idfamilia = int(familia[0])
            resultadosPar = (idfamilia, 0, 0.0, 0)
            preCepCad = str(int(familia[6]) // 1000)
            numEnderCad = familia[5]
            #print("OLHAE: ",numEnderCad)
            i = 0
            diceCoef = 0.0

            if preCepCad in conjuntoCidadeCnefeDic:
                while not (math.isclose(diceCoef, 1.0) and numEnderCad == numEndCnefe) and i < quantidadeConjCnefe[preCepCad]:
                    # for endereco in self.conjuntoCidadeCnefeDic[preCepCad]:
                    endereco = conjuntoCidadeCnefeDic[preCepCad][i]
                    numEndCnefe = endereco[4]

                    #endcf = self.formEndeCnefe
                    enderecoCnefe = [endereco[1], endereco[2], endereco[3]]

                    idEndereco = int(endereco[0])

                    diceCoef = self.similar.dice_coefficient1(','.join(enderecoCadUnic), ','.join(enderecoCnefe))
                    #print (numEndCnefe, numEnderCad)
                    if diceCoef >= resultadosPar[2]:
                        if diceCoef >= 0.95 and numEnderCad == numEndCnefe and (numEnderCad not in [None, 0]) and (numEndCnefe not in [None, 0]):
                            resultadosPar = (idfamilia, idEndereco, diceCoef, 5)

                        if diceCoef >= 0.95 and (numEnderCad in [None, 0]) and (numEndCnefe in [None, 0]):
                            resultadosPar = (idfamilia, idEndereco, diceCoef, 4)

                        if diceCoef >= 0.95 and numEnderCad != numEndCnefe and (numEnderCad not in [None, 0]) and (numEndCnefe not in [None, 0]):
                            resultadosPar = (idfamilia, idEndereco, diceCoef, 3)

                        if diceCoef < 0.95 and numEnderCad == numEndCnefe and (numEnderCad not in [None, 0]) and (numEndCnefe not in [None, 0]) :
                            resultadosPar = (idfamilia, idEndereco, diceCoef, 2)

                        if diceCoef < 0.95 and (numEnderCad in [None, 0]) and (numEndCnefe in [None, 0]):
                            resultadosPar = (idfamilia, idEndereco, diceCoef, 1)

                        if diceCoef < 0.95 and numEnderCad != numEndCnefe and (numEnderCad not in [None, 0]) and (numEndCnefe not in [None, 0]):
                            resultadosPar = (idfamilia, idEndereco, diceCoef, 0)

                    i = i + 1

            resp = [str(resultadosPar[0]), str(resultadosPar[1]), str(resultadosPar[2]), str(resultadosPar[3])]
            # self.respostasThreads[faixaCadUnico].append(resultadosPar)
            dbaseth.inseirdados(nomeSchemaResult, nomeTabelaResult, [resp])

    def armazenarRespostas(self, nomeSchema, tabelaResultado):
        bd = Dbtool("localhost", "5432", "mds_cad_unic", "postgres", "2631")
        for item in self.respostasThreads:
            bd.inseirdados(nomeSchema, tabelaResultado, item)

    def juntarTabelas(self, nomeSchemaCadUnic, tabelaCadUnic, nomeScehemaCnefe, tabelaCnefe):
        bd = Dbtool("localhost", "5432", "mds_cad_unic", "postgres", "2631")
        nomeTabelaResult = "resultado_" + nomeSchemaCadUnic.split("_")[2] + "_" + tabelaCnefe
        listaAtributos = ['cod_familiar_fam numeric', 'cod_unico_endereco integer', 'dice_coeficiente double precision', 'nivel_precisao integer']
        bd.criartabela("public", nomeTabelaResult, listaAtributos, 1)
        self.prepararDividirTarefa(nomeSchemaCadUnic, tabelaCadUnic, nomeScehemaCnefe, tabelaCnefe)
        #self.criarMascaraEnderecos()
        print("Criando processos =>\n")

        threads = []
        # executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.numThread, thread_name_prefix="Minion")
        for i in range(self.numThread):
            threads.append(Process(target=self.compararTabelas, name="deus",
                                   args=(nomeScehemaCnefe, tabelaCnefe, i, "public", nomeTabelaResult)))


        for i in range(self.numThread):
            threads[i].start()
            # threads[i].join()

        for i in range(self.numThread):
            threads[i].join()
            # self.compararTabelas(nomeScehemaCnefe,tabelaCnefe, i,"public", nomeTabelaResult)
            # print(threads[i])

        # executor.shutdown(wait=True)

    # concurrent.futures.wait(threads, timeout=None, return_when=ALL_COMPLETED)


if __name__ == '__main__':
    j = JuncaoTools(8)
    ini = time.time()
    #j.juntarTabelas("cad_unic_2019", "rr10000", "cnefe_rr_14", "14_rr")
    j.juntarTabelas("cad_unic_2019", "base_cad_unic_2019_35", "cnefe_sp_35", "35_sp")
    fim = time.time()
    print("\n=========================================\n")
    print("Tempo: " + str(fim - ini))
