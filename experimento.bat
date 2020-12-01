echo on
cls
echo “SCRIPT .BAT pra crusar bases de dados”



CADUNIC=cad_unic_2012.base_cad_unic_2012_ESQ.cad_unic_2012.base_cad_unic_2012_TAB

echo "Iniciando processamento do estado de RO"


python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_ro_11.11_ro -s resultado_11_ro_2012 -p -t 7
echo "Teminado o processamento do estado de RO"

echo "Iniciando processamento do estado de AC"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_ac_12.12_ac -s resultado_12_ac_2012 -t 7
echo "Teminado o processamento do estado de AC"

echo "Iniciando processamento do estado de AM"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_am_13.13_am -s resultado_13_am_2012 -t 7
echo "Teminado o processamento do estado de AM"

echo "Iniciando processamento do estado de RR"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_rr_14.14_rr -s resultado_14_rr_2012 -t 7
echo "Teminado o processamento do estado de RR"

echo "Iniciando processamento do estado de PA"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_pa_15.15_pa -s resultado_15_pa_2012 -t 7
echo "Teminado o processamento do estado de PA"

echo "Iniciando processamento do estado de AP"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_ap_16.16_ap -s resultado_16_ap_2012 -t 7
echo "Teminado o processamento do estado de AP"

echo "Iniciando processamento do estado de TO"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_to_17.17_to -s resultado_17_to_2012 -t 7
echo "Teminado o processamento do estado de TO"

echo "Iniciando processamento do estado de MA"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_ma_21.21_ma -s resultado_21_ma_2012 -t 7
echo "Teminado o processamento do estado de MA "

echo "Iniciando processamento do estado de PI"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_pi_22.22_pi -s resultado_22_pi_2012 -t 7
echo "Teminado o processamento do estado de PI "

echo "Iniciando processamento do estado de CE"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_ce_23.23_ce -s resultado_23_ce_2012 -t 7
echo "Teminado o processamento do estado de CE"

echo "Iniciando processamento do estado de RN"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_rn_24.24_rn -s resultado_24_rn_2012 -t 7
echo "Teminado o processamento do estado de RN"

echo "Iniciando processamento do estado de PB"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_pb_25.25_pb -s resultado_25_pb_2012 -t 7
echo "Teminado o processamento do estado de PB"

echo "Iniciando processamento do estado de PE"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_pe_26.26_pe -s resultado_26_pe_2012 -t 7
echo "Teminado o processamento do estado de PE"

echo "Iniciando processamento do estado de AL"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_al_27.27_al -s resultado_27_al_2012 -t 7
echo "Teminado o processamento do estado de AL"

echo "Iniciando processamento do estado de SE"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_se_28.28_se -s resultado_28_se_2012 -t 7
echo "Teminado o processamento do estado de SE"

echo "Iniciando processamento do estado de BA"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_ba_29.29_ba -s resultado_29_ba_2012 -p -t 7
echo "Teminado o processamento do estado de BA"

echo "Iniciando processamento do estado de MG"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_mg_31.31_mg -s resultado_31_mg_2012 -p -t 7
echo "Teminado o processamento do estado de MG"

echo "Iniciando processamento do estado de ES"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_es_32.32_es -s resultado_32_es_2012 -p -t 7
echo "Teminado o processamento do estado de ES"

echo "Iniciando processamento do estado de RJ"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_rj_33.33_rj -s resultado_33_rj_2012 -t 7
echo "Teminado o processamento do estado de RJ"

echo "Iniciando processamento do estado de PR"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_pr_41.41_pr -s resultado_41_pr_2012 -t 7
echo "Teminado o processamento do estado de PR"

echo "Iniciando processamento do estado de SC"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_sc_42.42_sc -s resultado_42_sc_2012 -t 7
echo "Teminado o processamento do estado de SC"

echo "Iniciando processamento do estado de RS"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_rs_43.43_rs -s resultado_43_rs_2012 -t 7
echo "Teminado o processamento do estado de RS"

echo "Iniciando processamento do estado de MS"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_ms_50.50_ms -s resultado_50_ms_2012 -t 7
echo "Teminado o processamento do estado de MS"

echo "Iniciando processamento do estado de MT"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_mt_51.51_mt -s resultado_51_mt_2012 -t 7
echo "Teminado o processamento do estado de MT"

echo "Iniciando processamento do estado de GO"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_go_52.52_go -s resultado_52_go_2012 -t 7
echo "Teminado o processamento do estado de GO"

echo "Iniciando processamento do estado de DF"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_df_53.53_df -s resultado_53_df_2012 -t 7
echo "Teminado o processamento do estado de DF"

echo "Iniciando processamento do estado de SP"
python ajuntamds.py -ca cad_unic_2012.base_cad_unic_2012 -cn cnefe_sp_35.35_sp -s resultado_35_sp_2012 -t 6
echo "Teminado o processamento do estado de SP "

echo "####################################################################################"
echo "Fim de processamento!"

