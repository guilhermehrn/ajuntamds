import psycopg2


class Dbtool:
    def __init__(self):

        self.host = "localhost"
        self.port = "5432"
        self.db = "mds-cad-unico"
        self.user = "postgres"
        self.password = "2631"

        self.conn = psycopg2.connect(
            " dbname=" + self.db + " user=" + self.user + " host=" + self.host + " password=" + self.password)

    def selecionarTabela(self, nometabela, atributos, condicoes, limitlinhas):

        sql = "select * "
        if type(atributos) is list:
            sql = "select " + ', '.join(atributos)

        sql = sql + " from " + ', '.join(nometabela)

        if condicoes != '':
            sql = sql + " where " + condicoes + " "

        if limitlinhas > 0:
            sql = sql + " " + "limit " + str(limitlinhas)
        sql = sql + ";"

        print(sql)
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()

        return rows

    def criartabela(self, nomeTabela, listaAtributos, temQueDropar):

        try:
            if temQueDropar:
                sql = "DROP TABLE IF EXISTS " + nomeTabela
                print(sql)
                cur = self.conn.cursor()
                cur.execute(sql)
                self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            # print("Erro ao dropar tabela " + nometabela)

        try:
            print(sql)
            sql = "CREATE TABLE " + nomeTabela
            sql = sql + "( " + ', '.join(listaAtributos) + " );"

            print(sql)
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def inseirdados(self, nomeTabela, intensParaInserir):
        sqlpart1 = "INSERT INTO " + nomeTabela + " VALUES "
        cur = self.conn.cursor()
        for row in intensParaInserir:
            sqlpart2 = "( " + ','.join(row) + ")"
            sql = sqlpart1 + sqlpart2
            try:
                cur.execute(sql)
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)

        self.conn.commit()


p = Dbtool()
tab = ["mgs as mg", "ibges as ib"]
col = ["cod_municipio", "cod_munic_ibge_5_fam"]
cod = ""
# r = p.selecionarTabela(tab, col, cod, 1)

p.criartabela('guilherme', ['id integer', 'nome varchar(255)'], 1)

dat = [['1', "'gui'"], ['2', "'goi'"]]
p.inseirdados('guilherme', dat)

r = p.selecionarTabela(["guilherme"], "* ", cod, 0)

print(r)
