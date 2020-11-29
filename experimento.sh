# !/bin/bash
command

# Scripit usado para rodar a base para todo o brasil. Construido para um ano

#As 5 seguintes variaveis deve ser setada pelo usuario
THREADS=7  # Aqui setar o numero de Threads de processador que será usado no processamento
CADUNIC_ESQ="cad_unic_2019"  #esquema do cadastro unico que será usado
CADUNIC_TAB="base_cad_unic_2019" #tabela do cadastro unico que será usado
ANO=2019 #ano dabase de cadastro unico
#=======================================================================================================

#A seguir começa o processamento da base de dados
#
CADUNIC=cad_unic_$ANO.base_cad_unic_$ANO_ESQ.cad_unic_$ANO.base_cad_unic_$ANO_TAB

echo "Iniciando processamento do estado de RO"
#python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_mg_31.31_mg -s resposta_contagem_2019 -t 6

python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_ro_11.11_ro -s resultado_11_ro_$ANO -t $THREADS
echo "Teminado o processamento do estado de RO"

echo "Iniciando processamento do estado de AC"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_ac_12.12_ac -s resultado_12_ac_$ANO -t $THREADS
echo "Teminado o processamento do estado de AC"

echo "Iniciando processamento do estado de AM"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_am_13.13_am -s resultado_13_am_$ANO -t $THREADS
echo "Teminado o processamento do estado de AM"

echo "Iniciando processamento do estado de RR"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_rr_14.14_rr -s resultado_14_rr_$ANO -t $THREADS
echo "Teminado o processamento do estado de RR"

echo "Iniciando processamento do estado de PA"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_pa_15.15_pa -s resultado_15_pa_$ANO -t $THREADS
echo "Teminado o processamento do estado de PA"

echo "Iniciando processamento do estado de AP"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_ap_16.16_ap -s resultado_16_ap_$ANO -t $THREADS
echo "Teminado o processamento do estado de AP"

echo "Iniciando processamento do estado de TO"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_to_17.17_to -s resultado_17_to_$ANO -t $THREADS
echo "Teminado o processamento do estado de TO"

echo "Iniciando processamento do estado de MA"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_ma_21.21_ma -s resultado_21_ma_$ANO -t $THREADS
echo "Teminado o processamento do estado de MA "

echo "Iniciando processamento do estado de PI"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_pi_22.22_pi -s resultado_22_pi_$ANO -t $THREADS
echo "Teminado o processamento do estado de PI "

echo "Iniciando processamento do estado de CE"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_ce_23.23_ce -s resultado_23_ce_$ANO -t $THREADS
echo "Teminado o processamento do estado de CE"

echo "Iniciando processamento do estado de RN"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_rn_24.24_rn -s resultado_24_rn_$ANO -t $THREADS
echo "Teminado o processamento do estado de RN"

echo "Iniciando processamento do estado de PB"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_pb_25.25_pb -s resultado_25_pb_$ANO -t $THREADS
echo "Teminado o processamento do estado de PB"

echo "Iniciando processamento do estado de PE"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_pe_26.26_pe -s resultado_26_pe_$ANO -t $THREADS
echo "Teminado o processamento do estado de PE"

echo "Iniciando processamento do estado de AL"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_al_27.27_al -s resultado_27_al_$ANO -t $THREADS
echo "Teminado o processamento do estado de AL"

echo "Iniciando processamento do estado de SE"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_se_28.28_se -s resultado_28_se_$ANO -t $THREADS
echo "Teminado o processamento do estado de SE"

echo "Iniciando processamento do estado de BA"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_ba_29.29_ba -s resultado_29_ba_$ANO -p -t $THREADS
echo "Teminado o processamento do estado de BA"

echo "Iniciando processamento do estado de MG"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_mg_31.31_mg -s resultado_31_mg_$ANO -p -t $THREADS
echo "Teminado o processamento do estado de MG"

echo "Iniciando processamento do estado de ES"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_es_32.32_es -s resultado_32_es_$ANO -p -t $THREADS
echo "Teminado o processamento do estado de ES"

echo "Iniciando processamento do estado de RJ"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_rj_33.33_rj -s resultado_33_rj_$ANO -t $THREADS
echo "Teminado o processamento do estado de RJ"

echo "Iniciando processamento do estado de PR"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_pr_41.41_pr -s resultado_41_pr_$ANO -t $THREADS
echo "Teminado o processamento do estado de PR"

echo "Iniciando processamento do estado de SC"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_sc_42.42_sc -s resultado_42_sc_$ANO -t $THREADS
echo "Teminado o processamento do estado de SC"

echo "Iniciando processamento do estado de RS"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_rs_43.43_rs -s resultado_43_rs_$ANO -t $THREADS
echo "Teminado o processamento do estado de RS"

echo "Iniciando processamento do estado de MS"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_ms_50.50_ms -s resultado_50_ms_$ANO -t $THREADS
echo "Teminado o processamento do estado de MS"

echo "Iniciando processamento do estado de MT"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_mt_51.51_mt -s resultado_51_mt_$ANO -t $THREADS
echo "Teminado o processamento do estado de MT"

echo "Iniciando processamento do estado de GO"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_go_52.52_go -s resultado_52_go_$ANO -t $THREADS
echo "Teminado o processamento do estado de GO"

echo "Iniciando processamento do estado de DF"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_df_53.53_df -s resultado_53_df_$ANO -t $THREADS
echo "Teminado o processamento do estado de DF"

echo "Iniciando processamento do estado de SP"
python3 ajuntamds.py -ca cad_unic_$ANO.base_cad_unic_$ANO -cn cnefe_sp_35.35_sp -s resultado_35_sp_$ANO -t $THREADS
echo "Teminado o processamento do estado de SP "

echo "####################################################################################"
echo "Fim de processamento!"

