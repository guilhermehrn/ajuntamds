import json
import psycopg2
import sys

ANO = 2013


if __name__ == '__main__':
    with open("config.json", "r") as json_file:
        dadosConexao = json.load(json_file)

    conn = psycopg2.connect("dbname=" + dadosConexao["base"] + " user=" + dadosConexao["user"] + " host=" + dadosConexao["host"] + " password=" + dadosConexao["password"])
    dic_uf = [(12,"AC"),(27,"AL"),(13,"AM"),(16,"AP"),(29,"BA"),(23,"CE"),(53,"DF"),(32,"ES"),(52,"GO"),(21,"MA"),(31,"MG"),(50,"MS"),(51,"MT"),(15,"PA"),(25,"PB"),(26,"PE"),(22,"PI"),(41,"PR"),(33,"RJ"),(24,"RN"),(11,"RO"),(14,"RR"),(43,"RS"),(42,"SC"),(28,"SE"),(35,"SP"),(17,"TO")]

    file_name = "./contab/resultado_consolidado_" + str(ANO) + ".csv"
    head = "UF, nivel 0, nivel 1, nivel 2, nivel 3, nivel 4, nivel 5, total_cad_unic, total_classificado, status\n"

    file = open(file_name,"w")
    file.write(head)

    for uf in dic_uf:
        name_tab = "resultado_" + str(uf[0]) + "_" + uf[1].lower() + "_" + str(ANO)

        sql = "SELECT nivel_precisao, count(*) FROM " + name_tab + " GROUP BY nivel_precisao ORDER  BY nivel_precisao ASC"

        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        list_aux = []
        num_inst_classif = 0

        for row in rows:
            list_aux.append(str(row[1]))
            num_inst_classif = num_inst_classif + int(row[1])

        name_tab_cad_unic = "cad_unic_" + str(ANO) + "." + "grupo_estado_base_cad_unic_" + str(ANO)

        sql_two = "SELECT quantidade FROM " + name_tab_cad_unic + " WHERE cod_munic_ibge_2_fam = " + str(uf[0])
        cur = conn.cursor()
        cur.execute(sql_two)
        rows = cur.fetchall()

        num_inst_cad = rows[0][0]
        status = "ERRO!"

        if num_inst_classif == num_inst_cad:
            status = "OK"

        line_csv = uf[1] + "," + ",".join(list_aux) + "," + str(num_inst_cad) + "," + str(num_inst_classif) + "," + status + "\n"

        file.write(line_csv)

    file.close()

    sys.exit(0)









