from dbtools import Dbtool


class PreProcessamento:
    def __init__(self, host, porta, dbase, user, password):
        self.host = host
        self.port = porta
        self.db = dbase
        self.user = user
        self.password = password

        self.db = Dbtool(self.host, self.port, self.db, self.user, self.password)

    def preProcessarBaseCnefe(self, nomeSchema, nomeTabela):
        nomeTabGrupoCidade = "grupo_cidades_" + nomeTabela
        capoAgrupamento = "cod_municipio"

        self.db.criarTabelaDeGrupos(nomeSchema, nomeTabGrupoCidade, nomeTabela, capoAgrupamento, 1)

        tabGrupoCidades = self.db.selecionarTabela(nomeSchema, [nomeTabGrupoCidade], ["*"], "", 0)

        for cidade in tabGrupoCidades:
            nometabelaCidade = nomeTabela + "_" + str(cidade[0])

            self.db.criarVewDeUmaTabela(nomeSchema, nometabelaCidade, nomeTabela, capoAgrupamento + " = " + str(cidade[0]),
                                        '')
    def preProcessarBaseCadUnico(self, nomeSchema, nomeTabela,):
        nomeTabGrupoEstado = "grupo_estado_" + nomeTabela
        capoAgrupamento = "cod_munic_ibge_2_fam"

        self.db.criarTabelaDeGrupos(nomeSchema, nomeTabGrupoEstado, nomeTabela, capoAgrupamento, 1)
        tabGrupoEstados = self.db.selecionarTabela(nomeSchema, [nomeTabGrupoEstado], ["*"], "", 0)

        for estado in tabGrupoEstados:
            nometabelaEstado= nomeTabela + "_" + str(estado[0])

            self.db.criarVewDeUmaTabela(nomeSchema, nometabelaEstado, nomeTabela, capoAgrupamento + " = " + str(estado[0]),
                                        '')


p = PreProcessamento("localhost", "5432", "mds-cad-unico", "postgres", "2631")
#p.preProcessarBaseCnefe("cnefe_rr_14", "14_rr")
p.preProcessarBaseCadUnico("cad_unic_2019","base_cad_unic_2019")
