import psycopg2


class Dbtool:
    def __init__(self, host, porta, dbase, user, password):

        self.host = host
        self.port = porta
        self.db = dbase
        self.user = user
        self.password = password

        self.conn = psycopg2.connect(
            " dbname=" + self.db + " user=" + self.user + " host=" + self.host + " password=" + self.password)

    def selecionarTabela(self, nomeSchema, nometabela, atributos, condicoes, limitlinhas):

        sql = "select * "
        if type(atributos) is list:
            sql = "select " + ', '.join(atributos)

        sql = sql + " from " + ', '.join([nomeSchema + '."' + x + '"' for x in nometabela])

        # print(sql)

        if condicoes != '':
            sql = sql + " where " + condicoes + " "

        if limitlinhas > 0:
            sql = sql + " " + "limit " + str(limitlinhas)
        sql = sql + ";"

        #print(sql)
        cur = self.conn.cursor()
        cur.execute(sql)
        # print(cur.description[0][0], cur.description[0][1] , cur.description[1][0], cur.description[1][1])
        rows = cur.fetchall()

        return rows

    def criartabela(self, nomeSchema, nomeTabela, listaAtributos, temQueDropar):

        try:
            if temQueDropar:
                sql = "DROP TABLE IF EXISTS " + nomeSchema + '."' + nomeTabela + '"'
                #print(sql)
                cur = self.conn.cursor()
                cur.execute(sql)
                self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            # print("Erro ao dropar tabela " + nometabela)

        try:
            # print(sql)
            sql = "CREATE TABLE " + nomeSchema + '."' + nomeTabela + '"'
            sql = sql + "( " + ', '.join(listaAtributos) + " );"

            # print(sql)
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def inseirdados(self, nomeSchema, nomeTabela, intensParaInserir):
        sqlpart1 = "INSERT INTO " + nomeSchema + '."' + nomeTabela + '"' + " VALUES "
        cur = self.conn.cursor()
        for row in intensParaInserir:
            sqlpart2 = "( " + ','.join(row) + ")"
            sql = sqlpart1 + sqlpart2
            try:
                cur.execute(sql)
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)

        self.conn.commit()

    def retornarColunasTypes(self, nomeSchema, nomeTabela):
        sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '" + nomeSchema + "' AND table_name = '" + nomeTabela + "';"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        d = dict(rows)
        return d

    def retornarColunasIndex(self, nomeSchema, nomeTabela):
        sql = "SELECT * FROM " + nomeSchema + '."' + nomeTabela + '"' + " LIMIT 1"
        cur = self.conn.cursor()
        cur.execute(sql)
        colNomes = []
        i = 0
        for col in cur.description:
            colNomes.append([col[0], i])
            i = i + 1
        return dict(colNomes)

    def criarVewDeUmaTabela(self, nomeSchema, nomeView, nomeTabela, condicao, outrasClausulas):
        select = "SELECT * FROM " + nomeSchema + '."' + nomeTabela + '" WHERE ' + condicao + " " + outrasClausulas
        sql = 'CREATE OR REPLACE VIEW ' + nomeSchema + '."' + nomeView + '" AS (' + select + ");"
        # print(sql)
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def criarIndex(self, nomeSchema, nomeTabela, nomeColuna, nomeIndex):

        sql = "CREATE INDEX " + nomeIndex + " ON " + nomeSchema + '."' + nomeTabela + '"(' + nomeColuna + " ASC);"

        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def criarTabelaDeGrupos(self, nomeSchema, nomeTabela, nomeTabelaOrigem, campoTabelaOrigem, temQueDropar):
        select = "SELECT " + campoTabelaOrigem + ", count (*) AS quantidade " + 'FROM ' + nomeSchema + '."' + nomeTabelaOrigem + '" GROUP BY ' + campoTabelaOrigem + " ORDER BY " + campoTabelaOrigem
        sql = "CREATE MATERIALIZED VIEW " + nomeSchema + '."' + nomeTabela + '" AS (' + select + ');'

        if temQueDropar:
            cur = self.conn.cursor()
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS " + nomeSchema + '."' + nomeTabela + '"')
            self.conn.commit()

        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def contarInstancias(self, nomeSchema, nomeTabela):
        sql = "SELECT count(*) FROM " + nomeSchema + "." + '"' + nomeTabela + '"' + ";"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return int(rows[0][0])

    def criarSquema(self, nomeSchema):
        sql = "CREATE SCHEMA IF NOT EXISTS" + nomeSchema
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()

    def criarTabelaControle(self):

        sql = 'CREATE TABLE IF NOT EXISTS tabela_controle (nomesquema VARCHAR (255), nometabela VARCHAR (255))'
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()

    def deletarDadoDaTabelaControle(self, nomeSchema, nomeTabela):

        sql = "DELETE FROM tabela_controle WHERE nomesquema =" + "'" + nomeSchema + "'" + " AND " + "nometabela =" + "'" + nomeTabela + "';"
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()


#p = Dbtool("localhost","5432","mds_cad_unic","postgres","2631")


# tab = ["mgs as mg", "ibges as ib"]
# col = ["cod_municipio", "cod_munic_ibge_5_fam"]
# cod = ""
# r = p.selecionarTabela("cnefe_rr_14", ["14_rr"],["*"] ,cod, 1)
#
# p.criartabela("public",'guilherme', ['id integer', 'nome varchar(255)'], 1)
#
# dat = [['1', "'gui'"], ['2', "'goi'"]]
# p.inseirdados("public",'guilherme', dat)
#

# r = p.selecionarTabela("public",["guilherme"], ["nome"], cod, 1)
# r = p.criarVewDeUmaTabela("cnefe_rr_14", "teste", "14_rr", "cod_municipio=27")
# r = p.criarIndex("cnefe_rr_14", "14_rr", "cod_municipio", "guil")
# r = p.retornarColunasTypes("public","guilherme")
# r = p.retornarColunasIndex("public","guilherme")
# r = p.criarTabelaDeGrupos("cnefe_rr_14","grupo" ,"14_rr", "cod_municipio")
#r = p.contarInstancias("public","guilherme")
#print(r)
