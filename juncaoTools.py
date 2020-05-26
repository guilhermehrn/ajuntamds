from dbtools import Dbtool
from similaridade import Similaridade


class JuncaoTools:
    def __init__(self):
        self.bd = Dbtool()
        self.similar = Similaridade()

    def printarLista(self, lista):
        print ("==>> Resutados finais :")
        for int in lista:
            print(int)

    def juncaoTabelas(self, tabelaA, tabelaB):

        print("Buscando os dados do baco.")
        self.conjuntoA = self.bd.selecionarTabela([tabelaA], ["*"], '', 15)
        self.conjuntoB = self.bd.selecionarTabela([tabelaB], ["*"], '', 0)

        #print("tamanho A:", len(self.conjuntoA))
        #print("tamanho B:", len(self.conjuntoB))

        self.indexColA = self.bd.retornarColunasIndex(tabelaA)
        self.tiposColA = self.bd.retornarColunasTypes(tabelaA)

        self.indexColB = self.bd.retornarColunasIndex(tabelaB)
        self.tiposColB = self.bd.retornarColunasTypes(tabelaB)

        resultadosPar = (1, 1, 0.0)

        resultadosFin = []

        #IBGE
        mEndA = [self.indexColA["nom_tip_logradouro_fam"],
                 self.indexColA["nom_tit_logradouro_fam"],
                 self.indexColA["nom_logradouro_fam"],
                 self.indexColA["num_logradouro_fam"],
                 #self.indexColA["des_complemento_fam"],
                 self.indexColA["nom_localidade_fam"],
                 self.indexColA["cod_munic_ibge_5_fam"],
                 self.indexColA["cod_munic_ibge_2_fam"]
                 ]

        #CNEFE
        mEndB = [self.indexColB["nom_tipo_seglogr"],
                 self.indexColB["nom_titulo_seglogr"],
                 self.indexColB["nom_seglogr"],
                 self.indexColB["num_endereco"],
                 #self.indexColB["dsc_modificador"],
                 self.indexColB["dsc_localidade"],
                 self.indexColB["cod_municipio"],
                 self.indexColB["cod_uf"]
                 ]
        print ("Fazendo a juncao.")
        count = 0
        for rowA in self.conjuntoA:
            resultadosPar = (1, 1, 0.0)
            enderecoA = [str(rowA[mEndA[0]]).replace("'",""), str(rowA[mEndA[1]]).replace("'",""), str(rowA[mEndA[2]]).replace("'",""),
                         str(rowA[mEndA[3]]).replace("'",""), str(rowA[mEndA[4]]).replace("'",""), str(rowA[mEndA[5]]).replace("'",""),
                         str(rowA[mEndA[6]]).replace("'","")
                         ]

            idA = int(rowA[self.indexColA["cod_familiar_fam"]])
            print("dados processados: " + str(count))
            for rowB in self.conjuntoB:

                enderecoB = [str(rowB[mEndB[0]]).replace("'",""), str(rowB[mEndB[1]]).replace("'",""), str(rowB[mEndB[2]]).replace("'",""),
                             str(rowB[mEndB[3]]).replace("'",""), str(rowB[mEndB[4]]).replace("'",""), str(rowB[mEndB[5]]).replace("'",""),
                             str(rowB[mEndB[6]]).replace("'","")
                             ]
                idb = int(rowB[self.indexColB["cod_unico_endereco"]])

                #print("END A:", ', '.join(enderecoA))
                #print("END B:", ', '.join(enderecoB))
                diceCoef = self.similar.dice_coefficient2(','.join(enderecoA), ','.join(enderecoB))

                if diceCoef > resultadosPar[2]:
                    resultadosPar = (idA, idb, diceCoef)
            count = count + 1

            resultadosFin.append(resultadosPar)

        #self.printarLista(resultadosFin)

        arq = open('saida.csv', 'w')


        arq.write("cod_familiar_fam; cod_unico_endereco; Dice_coeficiente\n")
        for lin in resultadosFin:
            arq.write(str(lin[0]) + ";" + str(lin[1]) + ";" + str(lin[2]) +"\n")
        arq.close()





j = JuncaoTools()
j.juncaoTabelas("cad_unic_bh", 'cnefe_bh')



# bucas a tabela A
# busca tabela B

# roda similaridade a [i] b[i..n]
# para cada (a[i] b[j]]) > 0,80 add na lista L
# pga o com maior similaridade
