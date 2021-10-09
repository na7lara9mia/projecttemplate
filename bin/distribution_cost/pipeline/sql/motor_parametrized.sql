(SELECT RBVQTLM1.cod_motor, RBVQTLM1.energia,  RBVQTTFF.libelle_francais
FROM df_rbvqtlm1 RBVQTLM1
JOIN df_rbvqttff RBVQTTFF ON RBVQTLM1.qi_filial = RBVQTTFF.qi_filial
WHERE RBVQTTFF.libelle_francais = '{2}')
