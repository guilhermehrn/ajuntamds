import math
import time

from dbtools import Dbtool
from similaridade import Similaridade
import concurrent.futures
from multiprocessing import Process
import os
from concurrent.futures import ALL_COMPLETED
import threading


class JuncaoTools:
    def __init__(self, numtheads):
        #self.bd = Dbtool("localhost", "5432", "mds_cad_unic", "postgres", "2631")
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


        self.mascaraEnderecoCadUnic = ["nom_tip_logradouro_fam",
                                       "nom_tit_logradouro_fam",
                                       "nom_logradouro_fam",
                                       "num_logradouro_fam",
                                       #"des_complemento_fam",
                                       "nom_localidade_fam",
                                       "cod_munic_ibge_5_fam",
                                       "cod_munic_ibge_2_fam" ]

        self.mascaraEnderecoCnefe = ["nom_tipo_seglogr",
                                     "nom_titulo_seglogr",
                                     "nom_seglogr",
                                     "num_endereco",
                                     #"dsc_modificador",
                                     "dsc_localidade",
                                     "cod_municipio",
                                     "cod_uf"]




    def printarLista(self, lista):
        print("==>> Resutados finais :")
        for int in lista:
            print(int)

    def prepararDividirTarefa(self, nomeSchemaCadUnic, tabelaCadUnic, nomeScehemaCnefe, tabelaCnefe):

        bd = Dbtool("localhost", "5432", "mds_cad_unic", "postgres", "2631")
        print("Buscando os dados do  cad unico no baco.")

        self.conjuntoCadUnic = bd.selecionarTabela(nomeSchemaCadUnic, [tabelaCadUnic], ["*"], '', 0)

        print("Buscando Indexadores.")

        self.indexColCadUnic = bd.retornarColunasIndex(nomeSchemaCadUnic, tabelaCadUnic)
        self.tiposColCadUnic = bd.retornarColunasTypes(nomeSchemaCadUnic, tabelaCadUnic)

        self.indexColCnefe = bd.retornarColunasIndex(nomeScehemaCnefe, tabelaCnefe)
        self.tiposColCnefe = bd.retornarColunasTypes(nomeScehemaCnefe, tabelaCnefe)

        print("Criando grupos de cidades.")
        nometabelagrupoAux = tabelaCadUnic + "aux"

        bd.criarTabelaDeGrupos(nomeSchemaCadUnic, nometabelagrupoAux, tabelaCadUnic, "cod_munic_ibge_5_fam", 1)

        self.numInstancidadesCadUnicDic = dict(bd.selecionarTabela(nomeSchemaCadUnic, [nometabelagrupoAux], ["*"], '', 0))
        self.codCidadesOrdenadaslist = list(self.numInstancidadesCadUnicDic.keys())

        self.numTotalInstancias = bd.contarInstancias(nomeSchemaCadUnic, tabelaCadUnic)

        self.numInstanciasCadUnicPorThread = self.numTotalInstancias // self.numThread
        aux = 0
        for i in range(self.numThread):
            self.respostasThreads.append([])

            if i == 0:
                self.tarefasParaTheads.append(self.conjuntoCadUnic[i: self.numInstanciasCadUnicPorThread])
                aux += self.numInstanciasCadUnicPorThread
            if i == (self.numThread - 1):
                self.tarefasParaTheads.append(self.conjuntoCadUnic[aux: self.numTotalInstancias])

            else:
                self.tarefasParaTheads.append(self.conjuntoCadUnic[aux: (aux + self.numInstanciasCadUnicPorThread)])
                aux = aux + self.numInstanciasCadUnicPorThread

    def criarMascaraEnderecos(self):

        for i in range(len(self.mascaraEnderecoCadUnic)):
            cad = self.mascaraEnderecoCadUnic[i]
            cenef = self.mascaraEnderecoCnefe[i]
            self.indexCadUnic.append(self.indexColCadUnic[cad])
            self.indexCnefe.append(self.indexColCnefe[cenef])

    def compararTabelas(self, nomeScehemaCnefe, tabelaCnefe, faixaCadUnico, nomeSchemaResult, nomeTabelaResult):

        cidadeCorrente = 0

        #conjuntoCidadeCnefe = []
        #resultadosPar = (1, 1, 0.0)
        diceCoef = 0.0

        dbaseth = Dbtool("localhost", "5432", "mds_cad_unic", "postgres", "2631")
        #print("MEU PAI E: ", os.getppid())

        for familia in self.tarefasParaTheads[faixaCadUnico]:

            if int(familia[5]) != cidadeCorrente:
                t = str(os.getpid())

                cidadeCorrente = int(familia[5])
                #print(t + ": buscando a Cidade " + str(cidadeCorrente))
                nometabelaCnefeCorrente = tabelaCnefe + "_" + str(cidadeCorrente)

                conjuntoCidadeCnefe = dbaseth.selecionarTabela(nomeScehemaCnefe, [nometabelaCnefeCorrente], ["*"], '', 0)
                conjuntoCidadeCnefeDic = {}
                quantidadeConjCnefe = {}

                #print(t + ": montando arvore para cidade " + str(cidadeCorrente))

                for linha in conjuntoCidadeCnefe:
                    preCepCnefe = str(int(linha[self.indexColCnefe["cep"]]) // 1000)
                    # lprefcep = str(int(familia[self.indexColCadUnic["num_cep_logradouro_fam"]])/1000)

                    if preCepCnefe in conjuntoCidadeCnefeDic:
                        conjuntoCidadeCnefeDic[preCepCnefe].append(linha)
                        quantidadeConjCnefe[preCepCnefe] = quantidadeConjCnefe[preCepCnefe] + 1
                    else:
                        conjuntoCidadeCnefeDic[preCepCnefe] = [linha]
                        quantidadeConjCnefe[preCepCnefe] = 1

                #print(t + ": comparando enderecos na cidade " + str(cidadeCorrente))

            enderecoCadUnic = [familia[self.indexCadUnic[0]], familia[self.indexCadUnic[1]],
                               familia[self.indexCadUnic[2]], str(familia[self.indexCadUnic[3]]),
                               familia[self.indexCadUnic[4]]
                               ]
            idfamilia = int(familia[self.indexColCadUnic["cod_familiar_fam"]])
            resultadosPar = (idfamilia, 0, 0.0)
            preCepCad = str(int(familia[self.indexColCadUnic["num_cep_logradouro_fam"]]) // 1000)
            i = 0
            diceCoef = 0.0

            if preCepCad in conjuntoCidadeCnefeDic:

                while not (math.isclose(diceCoef, 1.0)) and i < quantidadeConjCnefe[preCepCad]:
                    # for endereco in self.conjuntoCidadeCnefeDic[preCepCad]:
                    endereco = conjuntoCidadeCnefeDic[preCepCad][i]

                    enderecoCnefe = [endereco[self.indexCnefe[0]], endereco[self.indexCnefe[1]],
                                     endereco[self.indexCnefe[2]], str(endereco[self.indexCnefe[3]]),
                                     endereco[self.indexCnefe[4]]
                                     ]

                    idEndereco = int(endereco[self.indexColCnefe["cod_unico_endereco"]])

                    diceCoef = self.similar.dice_coefficient2(','.join(enderecoCadUnic), ','.join(enderecoCnefe))

                    if diceCoef > resultadosPar[2]:
                        resultadosPar = (idfamilia, idEndereco, diceCoef)

                    i = i + 1

            #resp = [str(resultadosPar[0]), str(resultadosPar[1]), str(resultadosPar[2])]
            self.respostasThreads[faixaCadUnico].append(resultadosPar)
            #dbaseth.inseirdados(nomeSchemaResult, nomeTabelaResult, [resp])

    def armazenarRespostas(self, nomeSchema, tabelaResultado):
        bd = Dbtool("localhost", "5432", "mds_cad_unic", "postgres", "2631")
        for item in self.respostasThreads:
            bd.inseirdados(nomeSchema,tabelaResultado, item)

    def juntarTabelas(self, nomeSchemaCadUnic, tabelaCadUnic, nomeScehemaCnefe, tabelaCnefe):
        bd = Dbtool("localhost", "5432", "mds_cad_unic", "postgres", "2631")
        nomeTabelaResult = "resultado_" + nomeSchemaCadUnic.split("_")[2] + "_" + tabelaCnefe
        listaAtributos = ['cod_familiar_fam numeric', 'cod_unico_endereco integer', 'dice_coeficiente double precision']
        bd.criartabela("public", nomeTabelaResult, listaAtributos, 1)
        self.prepararDividirTarefa(nomeSchemaCadUnic, tabelaCadUnic, nomeScehemaCnefe,tabelaCnefe)
        self.criarMascaraEnderecos()

        threads = []
        #executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.numThread, thread_name_prefix="Minion")
        for i in range(self.numThread):
            threads.append(Process(target=self.compararTabelas, name="deus", args=(nomeScehemaCnefe, tabelaCnefe, i, "public", nomeTabelaResult)))

        for i in range(self.numThread):
            threads[i].start()
            #threads[i].join()

        for i in range(self.numThread):
            threads[i].join()
            #self.compararTabelas(nomeScehemaCnefe,tabelaCnefe, i,"public", nomeTabelaResult)
            #print(threads[i])

        #executor.shutdown(wait=True)

    #concurrent.futures.wait(threads, timeout=None, return_when=ALL_COMPLETED)







if __name__ == '__main__':
    j = JuncaoTools(30)
    ini = time.time()
    j.juntarTabelas("cad_unic_2019", "rr10000", "cnefe_rr_14", "14_rr")
    fim = time.time()
    print("\n=========================================\n")
    print("Tempo: " + str(fim - ini))
