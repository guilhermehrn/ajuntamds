from dbtools import Dbtool
import json


class PreProcessamento:
    """Classe usada no preprocessamento das bases de dados.
    Ela preprocessa um estado do Brasil por vez.
    Deve ser rodada de toda a vez que a base for usada pela primeira vez
    """

    def __init__(self):

        with open("config.json", "r") as json_file:
            dadosConexao = json.load(json_file)

        self.db = Dbtool(dadosConexao["host"], dadosConexao["port"], dadosConexao["base"], dadosConexao["user"],
                         dadosConexao["password"])

    def preProcessarBaseCnefe(self, nomeSchema, nomeTabela):

        nomeTabGrupoCidade = "grupo_cidades_" + nomeTabela
        capoAgrupamento = "cod_municipio"

        self.db.deletarDadoDaTabelaControle(nomeSchema, nomeTabela)

        self.db.criarTabelaDeGrupos(nomeSchema, nomeTabGrupoCidade, nomeTabela, capoAgrupamento, 1)

        tabGrupoCidades = self.db.selecionarTabela(nomeSchema, [nomeTabGrupoCidade], ["*"], "", 0)

        for cidade in tabGrupoCidades:
            nometabelaCidade = nomeTabela + "_" + str(cidade[0])

            self.db.criarVewDeUmaTabela(nomeSchema, nometabelaCidade, nomeTabela,
                                        capoAgrupamento + " = " + str(cidade[0]),
                                        '')
        aux = ["'" + nomeSchema + "'", "'" + nomeTabela + "'"]
        self.db.inseirdados("public", "tabela_controle", [aux])

    def preProcessarBaseCadUnico(self, nomeSchema, nomeTabela):
        #print(nomeSchema)
        #print (nomeTabela)
        nomeTabGrupoEstado = "grupo_estado_" + nomeTabela
        capoAgrupamento = "cod_munic_ibge_2_fam"

        self.db.deletarDadoDaTabelaControle(nomeSchema, nomeTabela)

        self.db.criarTabelaDeGrupos(nomeSchema, nomeTabGrupoEstado, nomeTabela, capoAgrupamento, 1)

        tabGrupoEstados =self.db.selecionarTabela(nomeSchema, [nomeTabGrupoEstado], ["*"], "", 0)

        for estado in tabGrupoEstados:
            nometabelaEstado = nomeTabela + "_" + str(estado[0])

            self.db.criarVewDeUmaTabela(nomeSchema, nometabelaEstado, nomeTabela,
                                        capoAgrupamento + " = " + str(estado[0]),
                                        'ORDER BY cod_munic_ibge_5_fam ASC')

        aux = ["'" + nomeSchema + "'", "'" + nomeTabela + "'"]
        self.db.inseirdados("public", "tabela_controle", [aux])

    def testarseProcessado(self, nomeSchema, nomeTabela):

        self.db.criarTabelaControle();

        res = self.db.selecionarTabela("public", ["tabela_controle"], ["*"],
                                       "nomesquema= " + "'" + nomeSchema + "'" + " AND" + " nometabela=" + "'" + nomeTabela + "'",
                                       0)

        if not res:
            return False
        else:
            return True
