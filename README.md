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
 - Pelo menos 4GB de memória RAM.
 - Processador Quad-Core ou superior.
 
 Obs.: A quantidade de memória e de cores de processador vai influenciar diretamente a quantidade de Theads que o 
 programa irar executar o programa. Portanto quanto maior a quantidade de threads, mais núcleos de processador e memória 
 RAM exigirá, e mais rápido o programa tende a processar os dados.

## Carregando a base de dados
A importação dos dados deve ser feita de forma semiautomática utilizado alguma ferramenta ETL (Extract Transform Load) 
ou através do utilitário Pgadmin que vem junto com o PostgreSQL. Para mais informações: 
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
Tal arquivo deve ser preenchido do com as informações do banco no lugar dos espaços de nome.
## Execução do Programa

Basicamente o programa funciona de duas etapas: (1) Pré-processamento das bases de dados e (2) A junção das bases de dados. Sendo que a primeira etapa só precisa ser feita uma única vez, quando quando as bases forem utilizadas pela primeira vez.

Para a execução do programa basta digitar a seguinte linha de comando:

```sh
python ajuntamds.py -ca cad_unic_<ano>.base_cad_unic_2019 -cn cnefe_<sigla_est>_<cod_est>.<cod_est>_<sigla_est>
```

Por exemplo, para fazer a correlação das bases do Cadastro Único com o Cnefe para o estado de São Paulo do ano 2019 deve ser feito o seguinte:

```sh
python ajuntamds.py -ca cad_unic_2019.base_cad_unic_2019 -cn cnefe_35_sp.35_sp
```
Para obter a relação de todos os parametros de linha de comando que podem ser ultilizados, consulte a [wiki](https://github.com/guilhermehrn/ajuntamds/wiki).

### Pre-Processamento das bases de dados

O pré-processamento de bases de dados nada mais é que a criação de tabelas e views auxiliares necessárias para o funcionamento do 
programa. Esse pré-processamento é feito de forma automática caso as bases ainda não tenham sido processadas.

Porem, caso queira forçar pré-processamento, basta adicionar o parâmetro "-p":  

```sh
python ajuntamds.py -p -ca cad_unic_2019.base_cad_unic_2019 -cn cnefe_35_sp.35_sp
```

#### Calculando a quantidade de Theads

O AjuntaMDS realiza os processamentos em paralelo, ou seja utilizando mais de uma linha de execução ou Threads.
para definir a quantidade de theads que serão usados no processamento basta usar o parâmetro "-t" seguido do número de theads.
Por exemplo para executar o programa com 8 linhas de execução simultânea temos o seguinte: 

```sh
python ajuntamds.py -p -ca cad_unic_2019.base_cad_unic_2019 -cn cnefe_35_sp.35_sp -t 8
```

Aqui deve-se atentar aos recursos de processador e memória que a máquina oferece, pois se colocar um número de threads superior ao 
que a máquina oferece, o desempenho do programa fica comprometido pois começa a ter uma concorrência por recursos de processador.
Além disso aumenta a chaces da memória estourar. Então recomenda-se que deixe pelo menos 1,5 GB de memória para cada Thread.

Caso esse parâmetro não seja passado ele vai funcionar com 2 theads simultâneas.

### Interpretando os resultados
O Programa irá gerar uma tabela contendo para cada instância o código da Família, o código do endereço, coeficiente de Dice, e o nível de precisão, 
como podemos ver a seguir:

| cod_familiar_fam | cod_unico_endereco | dice_coeficiente | nivel_precisao |
|:----------------:|:------------------:|-----------------:|:--------------:|
|          1111111 |          222222222 |              1.0 |              5 |
|          3333333 |          444444444 |            0.888 |              3 |
|          4444444 |          223333333 |            0.955 |              4 |

Tal tabela é o que chamamos de tabela de junção, pois  pode-se correlacionar através do campo "cod_familiar_fam"
com as bases do Cadastro Único, bem como, pode-se correlacionar com as bases do CNEFE através do campo "cod_unico_endereco". Logo, 
consequentemente os dados do cadastro único são correlacionados com os dados do CNEFE.

#### O Nível de Precisão
Para ter uma forma básica de avaliarmos a qualidade da correlacão entre as instâncias do Cadastro Único  com as instâncias 
do CNFE foi criado grupos de resultados ou níveis de precisão dos resultados. 

Esses níveis de precisão vão de uma escala de 0 a 5, sendo 0 (zero) com menor nível de precisão ou menor qualidade das 
respostas, e 5 maior nível de precisão ou melhor qualidade na resposta. A relação dos níveis de precisão se dá da 
seguinte forma:

* D = igual ao Coeficiente de Dice.
* Eu = número do endereço no cadastro único.
* Ec = número do endereço no CNEFE.

Então temos os seguintes níveis de precisão:

* 0 : D < 95% e Eu ≠ Ec e Eu ≠ 0 e Ec ≠ 0
* 1 : D < 95% e Eu = Ec e Eu = 0 e Ec = 0
* 2 : D < 95% e Eu = Ec e Eu ≠ 0 e Ec ≠ 0 
* 3 : D > 95% e Eu ≠ Ec e Eu ≠ 0 e Ec ≠ 0
* 4 : D > 95% e Eu ≠ Ec e Eu = 0 e Ec = 0
* 5 : D > 95% e Eu = Ec e Eu ≠ 0 e Ec ≠ 0

Por exemplo, na tabela acima vemos, vemos que a primeira instancia tem nível de precisão igual a 5 pois o número do
endereço do cadastro único é igual ao que está presente no CNEFE e ambos são diferentes de zero e o os endereços de ambas 
as bases são idênticos apresentando um coeficiente de similaridade (Coeficiente de Dice)  igual a 1 (ou 100%).


### Histórico de Releases

* 1.0:
    *Início de projeto, protótipo dos algoritmos

* 2.0:
    * Paralelização do programa.
    * Troca da implementação do algoritmo de Dice 

* 2.1, 2.2, 2.3, 2.4:
    * Melhorias no código.
    * Melhorias no  processo de distibuição de tarefas para as threads.
    
* 2.5: 
    * Implementação dos paramentros de linha de comando de usuário
    * Correção de bugs.



