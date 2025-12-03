with vw_origem as ( ---possui 29125 linhas
    select
        ism.id_tempo,
        ism.id_status,
        st.status,
        m.id_matricula,
        m.id_projeto,
        eic.data_inicio_curso,
        eic.data_prevista_termino_curso,
        eic.informacoes_contrato,
        t.mes,
        t.ano,
        case 
            when YEAR(eic.data_inicio_curso) = t.ano then 'CALOURO'
            else 'VETERANO'
        end as tipo_aluno,
        case when (ism.id_tempo = (select max(ism2.id_tempo)-1 from ismart_status_mensal ism2) and ism.id_status in (7,9,10)) then 'UNI_HJ' else 'UNI' end as flg_universitario,
        case when (ism.id_tempo = (select max(ism2.id_tempo)-1 from ismart_status_mensal ism2) 
                          and ism.id_status in (7,9,10) 
                          and eic.data_prevista_termino_curso>= CAST(GETDATE() AS date)) then 'ATIVO' else 'N ATIVO' end as flg_ativo_nao_ativo,
        case 
            -- Este cria uma rega para analisar todos os casos de calouros e veteranos para a regra e caso não seja satisfatório ele deixa como 'NADA'
            -- CALOURO_HJ
            when  ism.id_tempo = (select max(ism2.id_tempo)-1 from ismart_status_mensal ism2)
            and ism.id_status in (7,9,10)
            and YEAR(eic.data_inicio_curso) = t.ano
            then 'CALOURO_HJ'
            -- VETERANO_HJ
            when  ism.id_tempo = (select max(ism2.id_tempo)-1 from ismart_status_mensal ism2)
            and ism.id_status in (7,9,10)
            and YEAR(eic.data_inicio_curso) <> t.ano
            then 'VETERANO_HJ'
            -- TODO O RESTO
            else 'NADA'
            end as flg_calouro_veterano,
        case 
            -- Este cria uma rega para analisar todos os casos de calouros e veteranos para a regra e caso não seja satisfatório ele deixa como 'NADA'
            -- CALOURO_ENTRANTES HOJE
            when  ism.id_tempo = (select max(ism2.id_tempo)-1 from ismart_status_mensal ism2)
            and YEAR(eic.data_inicio_curso)=2025
            and YEAR(eic.data_inicio_curso) = t.ano
            then 'CALOURO_ENTRANTES' ELSE 'NADA'
            end as flg_calouro_entrantes,

        case 
            -- Este case cria uma regra para analisar todos os casos de calouros e veteranos desligados e caso não seja deixa como 'NADA'
            -- CALOURO_DESLIGADOS
            when  ism.id_tempo = (select max(ism2.id_tempo)-1 from ismart_status_mensal ism2)
            and ism.id_status = 2
            and YEAR(eic.data_inicio_curso) = t.ano
            then 'CALOURO_DESLIG'
            -- VETERANO_DESLIGADOS
            when  ism.id_tempo = (select max(ism2.id_tempo)-1 from ismart_status_mensal ism2)
            and ism.id_status = 2
            and YEAR(eic.data_inicio_curso) <> t.ano
            then 'VETERANO_DESLIG'
            -- TODO O RESTO
            else 'NADA'
            end as flg_desligados,

        case 
            -- Este case cria uma regra para analisar todos os casos de calouros e veteranos desistentes e caso não seja deixa como 'NADA'
            -- CALOURO_DESLIGADOS
            when  ism.id_tempo = (select max(ism2.id_tempo)-1 from ismart_status_mensal ism2)
            and ism.id_status = 11
            and YEAR(eic.data_inicio_curso) = t.ano
            then 'CALOURO_DESIST'
            ELSE 'NADA'
            END as flg_desistente,
    case 
            -- Este cria uma rega para analisar todos os casos de calouros e veteranos para formandos até hoje se não for satisfatorio ele deixa como 'NADA'
            -- CALOURO_ENTRANTES HOJE
            when  ism.id_tempo = (select max(ism2.id_tempo)-1 from ismart_status_mensal ism2)
            AND ism.id_status=8
            then 'FORMANDOS'
            ELSE 'NADA'
            END AS flg_formandos       

    from ismart_status_mensal as ism
    left join ismart_status as st on st.id_status = ism.id_status
    left join ismart_matricula as m on m.id_matricula = ism.id_matricula
    left join data_facts_es_informacoes_curso as eic on eic.ra = ism.ra
    left join tempo as t on t.id_tempo = ism.id_tempo
    where m.id_projeto = 3
      and eic.informacoes_contrato = 1)

select * from vw_origem