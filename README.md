# Ajuntamds
## Introdução
O AjuntaMDS é um programa desenvolvido para a correlação entre as bases de dados do CNEFE com as bases 
de dados do Cadastro Único. Possibilitando o cruzamento de dados das famílias com os seus respectivos 
endereços atualizado, bem como ao longo do tempo.

Basicamente o AjuntaMDS se utiliza do algoritmo de DICE para fazer a comparação das informações e com 
isso fazer o devido cruzamento. 


## Requisitos do sistema

Para o funcionamento do programa deve-se atentar aos seguintes requisitos:

 - Pyhton 3.x ou superior.
 - PostgreSQL 10.x ou superior
 - Windows 10, ubuntu 16.04, ubuntu sever 16.04 ou superior.
 - Pelo menos 4GB de memoria RAM.
 - Processador Quad-Core ou superior.
 
 Obs.: A quantidade de memoria e de cores de processador vai influenciar diretamente a quantidade de Theads que o 
 programa irar executar o programa.Portanto quanto maior a quantidade de threads, mais núcleos de processador e memoria 
 RAM irá exigir, e mais rapido o programa tende a processar os dados.

## Carregando a base de dados
A importação dos dados deve ser feita de forma semiautomatica utilizado alguma ferramenta ETL (Extract Transform Load) 
ou através do utilitário Pdadmin que vem junto com o PostgreSQL. Para mais informações: 
https://blog.tecnospeed.com.br/backup-e-restore-postgresql/ 

Para tal deve-se atentar para o padrão adotado nas nomeclaturas de tabelas e esquemas do banco:
 * Cadastro Único (para cada ano):
     * Esquema: cad_unic_< ano >
     * Tabela: base_cad_unic_< ano >
     * Exemplo - Para o ano de 2019 tem-se:  
     ```sql
   CREATE SCHEMA cad_unic_2019
   CREATE TABLE base_cad_unic_2019 (...)
   ```
 * Base do CNEFE:
    * Esquema: cnefe_< sigla-uf >_< code-ibge >
    * Tabela: < code-ibge >_< sigla-uf >
    * Exemplo - Para o estado de São Paulo, cuja a sígla é SP e o código do IBGE é  35, tem-se:
     
   ```sql
   CREATE SCHEMA cnefe_sp_35
   CREATE TABLE 35_sp (...)
   ```

     

## Configuração do programa
No arquivo config.json tem-se os dados de configuração do banco de dados:
 ```json5
{
  "host": "<nome_do_host>",
  "port": "<número_da_porta>",
  "base": "<nome_banco>",
  "user": "<nome_de_usuário>",
  "password": "<senha>"
}
```
Tal aqruivo deve ser preenchido do com as informações do banco no lugar dos espaços de nome.
## Execução do Programa

Basicamente o programa funciona de duas etapas: (1) Pré-processamento das bases de dados e (2) A juncão das bases de dados. Sendo que a primeira etapa só precisa ser feita uma unica vez, quando quando as bases forem utilizadas pela primeira vez.

Para a execução do programa basta digitar a seguinte linha de comando:

```sh
python ajuntamds.py -ca cad_unic_<ano>.base_cad_unic_2019 -cn tabela -cn cnefe_<sigla_est>_<cod_est>.<cod_est>_<sigla_est>
```

### Pre-Processamento das bases de dados

### Junção das Bases de dados

#### Calculando a quantidade de Theads
