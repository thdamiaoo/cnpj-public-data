WITH socio_pj
     AS (SELECT A.st_cnpj_base,
                A.cd_tipo,
                A.st_nome,
                A.st_cpf_cnpj,
                A.cd_qualificacao,
                B.st_qualificacao                   AS qualificacao_socio,
                A.dt_entrada,
                A.cd_pais,
                D.st_pais                           AS pais,
                A.st_representante,
                A.st_nome_representante,
                A.cd_qualificacao_representante,
                C.st_qualificacao                   AS qualificacao_representante,
                A.cd_faixa_etaria
         FROM   PUBLIC.tb_socio A
                INNER JOIN PUBLIC.tb_qualificacao_socio B
                        ON B.cd_qualificacao = A.cd_qualificacao
                INNER JOIN PUBLIC.tb_qualificacao_socio C
                        ON C.cd_qualificacao = A.cd_qualificacao
                INNER JOIN PUBLIC.tb_pais D
                        ON D.cd_pais = A.cd_pais
         WHERE  A.cd_tipo = '1'
        --  and A.st_cnpj_base = '10742854'
        ),
     socio_estrangeiro
     AS (SELECT A.st_cnpj_base,
                A.cd_tipo,
                A.st_nome,
                A.st_cpf_cnpj,
                A.cd_qualificacao,
                B.st_qualificacao                   AS qualificacao_socio,
                A.dt_entrada,
                A.cd_pais,
                D.st_pais                           AS pais,
                A.st_representante,
                A.st_nome_representante,
                A.cd_qualificacao_representante,
                C.st_qualificacao                   AS qualificacao_representante,
                A.cd_faixa_etaria
         FROM   PUBLIC.tb_socio A
                INNER JOIN PUBLIC.tb_qualificacao_socio B
                        ON B.cd_qualificacao = A.cd_qualificacao
                INNER JOIN PUBLIC.tb_qualificacao_socio C
                        ON C.cd_qualificacao = A.cd_qualificacao
                INNER JOIN PUBLIC.tb_pais D
                        ON D.cd_pais = A.cd_pais
         WHERE  A.cd_tipo = '3'
         ),
	numero_filiais
	AS (SELECT st_cnpj_base,
       	      Count(st_cnpj_base)		            AS quantidade_empresa
	    FROM   PUBLIC.tb_estabelecimento
	  --    where st_cnpj_base = '10742854'
	    GROUP  BY st_cnpj_base 
	   )
SELECT B.st_cnpj_base                                                 AS cnpj_base,
       B.st_cnpj_ordem                                                AS cnpj_ordem,
       Concat(B.st_cnpj_base, B.st_cnpj_ordem, B.st_cnpj_dv)          AS cnpj,
       CASE
         WHEN B.cd_matriz_filial = '1' THEN 'Matriz'
         ELSE 'Filial'
       END                                                            AS matriz_filial,
       M.quantidade_empresa									AS quantidade_empresa,
       Cast(Replace(A.vl_capital_social, ',', '.') AS DECIMAL(15, 2)) AS capital_social,
       A.st_razao_social                                              AS razao_social,
       B.st_nome_fantasia                                             AS nome_fantasia,
       G.st_natureza_juridica                                         AS natureza_juridica,
       B.cd_situacao_cadastral                                        AS codigo_situacao_cadastral,
       B.dt_situacao_cadastral                                        AS data_situacao_cadastral,
       F.st_motivo_situacao_cadastral                                 AS motivo_situacao_cadastral,
       B.st_cidade_exterior                                           AS cidade_exterior,
       E.st_pais                                                      AS pais,
       B.dt_inicio_atividade                                          AS data_inicio_atividade,
       B.cd_cnae_principal                                            AS cnae_principal,
       C.st_cnae                                                      AS descricao_cnae,
       B.cd_cnae_secundario                                           AS cnae_secundario,
       B.st_tipo_logradouro                                           AS tipo_logradouro,
       B.st_logradouro                                                AS logradouro,
       B.st_numero                                                    AS numero,
       TRIM(B.st_complemento)                                         AS complemento,
       B.st_bairro                                                    AS bairro,
       B.st_cep                                                       AS cep,
       B.st_uf                                                        AS uf,
       D.st_municipio                                                 AS municipio,
       CASE
         WHEN B.st_uf IN ( 'AC', 'AL', 'AP', 'AM',
                           'BA', 'CE', 'DF', 'ES',
                           'GO', 'MA', 'MT', 'MS',
                           'MG', 'PA', 'PB', 'PR',
                           'PE', 'PI', 'RJ', 'RN',
                           'RS', 'RO', 'RR', 'SC',
                           'SP', 'SE', 'TO' ) THEN 'Brasil'
         ELSE ''
       END                                                            AS pais,
       B.st_ddd1                                                      AS ddd1,
       B.st_telefone1                                                 AS telefone1,
       B.st_ddd2                                                      AS ddd2,
       B.st_telefone2                                                 AS telefone2,
       B.st_ddd_fax                                                   AS ddd_fax,
       B.st_fax                                                       AS fax,
       LOWER(B.st_email)                                              AS email,
       B.st_situacao_especial                                         AS situacao_especial,
       B.dt_situacao_especial                                         AS data_situacao_especial,
       I.st_nome                                                      AS nome_socio_adm,
       J.cd_qualificacao										      AS codigo_qualificacao_socio_adm,
       J.st_qualificacao                                              AS qualificacao_socio_adm,
       K.st_nome                                                      AS empresa_socia,
       K.st_cpf_cnpj                                                  AS cnpj_empresa_socia,
       K.qualificacao_socio                                           AS qualificacao_empresa_socio,
       K.pais                                                         AS pais_empresa_socia,
       K.st_nome_representante                                        AS representante_empresa_socia,
       L.st_nome                                                      AS socio_estrangeiro,
       L.qualificacao_socio                                           AS qualificacao_socio_estrangeiro,
       L.pais                                                         AS pais_socio_estrangeiro,
       L.st_nome_representante                                        AS nome_socio_estrangeiro,
       now()												          AS create_at
FROM   PUBLIC.tb_empresa A
       INNER JOIN PUBLIC.tb_estabelecimento B
              ON A.st_cnpj_base = B.st_cnpj_base
       LEFT JOIN PUBLIC.tb_cnae C
              ON B.cd_cnae_principal = C.cd_cnae
       LEFT JOIN PUBLIC.tb_municipios D
              ON D.cd_municipio = B.cd_municipio
       LEFT JOIN PUBLIC.tb_pais E
              ON E.cd_pais = B.cd_pais
       LEFT JOIN PUBLIC.tb_motivo_situacao_cadastral F
              ON F.cd_motivo_situacao_cadastral = B.cd_motivo_situacao_cadastral
       LEFT JOIN PUBLIC.tb_natureza_juridica G
              ON A.cd_natureza_juridica = G.cd_natureza_juridica
       LEFT JOIN PUBLIC.tb_dados_simples H
              ON H.st_cnpj_base = A.st_cnpj_base
       INNER JOIN PUBLIC.tb_socio I
              ON I.st_cnpj_base = A.st_cnpj_base
       LEFT JOIN PUBLIC.tb_qualificacao_socio J
              ON I.cd_qualificacao = J.cd_qualificacao
       LEFT JOIN socio_pj k
              ON K.st_cnpj_base = A.st_cnpj_base
       LEFT JOIN socio_estrangeiro L
              ON L.st_cnpj_base = A.st_cnpj_base
       LEFT JOIN numero_filiais M
       	      ON M.st_cnpj_base = A.st_cnpj_base 
WHERE  B.cd_situacao_cadastral = '02' -- 02 Ativa
       AND J.cd_qualificacao = '49' -- Sócio ADM
       AND ( C.cd_cnae = '4636202' -- cnae's buscados
          OR B.cd_cnae_secundario = '4636202' );
    --    AND A.st_cnpj_base = '10742854'; 