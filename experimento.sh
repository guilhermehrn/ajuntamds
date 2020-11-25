# !/bin/bash


# Scripit usado para rodar a base para todo o brasil. Construido para um ano

#As 5 seguintes variaveis deve ser setada pelo usuario
THREADS=7  # Aqui setar o numero de Threads de processador que será usado no processamento
CADUNIC_ESQ=cad_unic_2019  #esquema do cadastro unico que será usado
CADUNIC_TAB=base_cad_unic_2019 #tabela do cadastro unico que será usado
ANO=2019 #ano dabase de cadastro unico
#=======================================================================================================

#A seguir começa o processamento da base de dados

CAUNIC=$CADUNIC_ESQ.$CADUNIC_TAB


#python3 ajuntamds.py -ca $CADUNIC -cn cnefe_ma_21.21_ma -s resultado_21_ma_$ANO -t $THREADS
#python3 ajuntamds.py -ca $CADUNIC -cn cnefe_pi_22.22_pi -s resultado_22_pi_$ANO -t $THREADS
#python3 ajuntamds.py -ca $CADUNIC -cn cnefe_ce_23.23_ce -s resultado_23_ce_$ANO -t $THREADS
#python3 ajuntamds.py -ca $CADUNIC -cn cnefe_rn_24.24_rn -s resultado_24_rn_$ANO -t $THREADS
#python3 ajuntamds.py -ca $CADUNIC -cn cnefe_pb_25.25_pb -s resultado_25_pb_$ANO -t $THREADS
#python3 ajuntamds.py -ca $CADUNIC -cn cnefe_pe_26.26_pe -s resultado_26_pe_$ANO -t $THREADS
#python3 ajuntamds.py -ca $CADUNIC -cn cnefe_al_27.27_al -s resultado_27_al_$ANO -t $THREADS
#python3 ajuntamds.py -ca $CADUNIC -cn cnefe_se_28.28_se -s resultado_28_se_$ANO -t $THREADS
#python3 ajuntamds.py -ca $CADUNIC -cn cnefe_ro_11.11_ro -s resultado_11_ro_$ANO -t $THREADS
#python3 ajuntamds.py -ca $CADUNIC -cn cnefe_29_ba.29_ba -s resultado_29_ba_$ANO -t $THREADS
#python3 ajuntamds.py -ca $CADUNIC -cn cnefe_rj_33.33_rj -s resultado_33_rj_$ANO -t $THREADS
#python3 ajuntamds.py -ca $CAUNIC -cn cnefe_df_53.53_df -s resultado_53_df_$ANO -t  $THREADS

