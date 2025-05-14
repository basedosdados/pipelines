with
    saeb_pivot as (
        select *
        from
            (
                select
                    ano,
                    id_escola,
                    id_aluno,
                    serie,
                    disciplina,
                    case
                        when ano in (2007, 2009) then 1 else peso_aluno
                    end as peso_aluno,
                    proficiencia_saeb,
                    case
                        when
                            (
                                serie = 5
                                and disciplina = 'LP'
                                and proficiencia_saeb < 150
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'LP'
                                and proficiencia_saeb < 200
                            )
                        then 1
                        when
                            (
                                serie = 5
                                and disciplina = 'MT'
                                and proficiencia_saeb < 175
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'MT'
                                and proficiencia_saeb < 225
                            )
                        then 1
                        else 0
                    end as insuficiente,
                    case
                        when
                            (
                                serie = 5
                                and disciplina = 'LP'
                                and proficiencia_saeb >= 150
                                and proficiencia_saeb < 200
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'LP'
                                and proficiencia_saeb >= 200
                                and proficiencia_saeb < 275
                            )
                        then 1
                        when
                            (
                                serie = 5
                                and disciplina = 'MT'
                                and proficiencia_saeb >= 175
                                and proficiencia_saeb < 225
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'MT'
                                and proficiencia_saeb >= 225
                                and proficiencia_saeb < 300
                            )
                        then 1
                        else 0
                    end as basico,
                    case
                        when
                            (
                                serie = 5
                                and disciplina = 'LP'
                                and proficiencia_saeb >= 200
                                and proficiencia_saeb < 250
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'LP'
                                and proficiencia_saeb >= 275
                                and proficiencia_saeb < 325
                            )
                        then 1
                        when
                            (
                                serie = 5
                                and disciplina = 'MT'
                                and proficiencia_saeb >= 225
                                and proficiencia_saeb < 275
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'MT'
                                and proficiencia_saeb >= 300
                                and proficiencia_saeb < 350
                            )
                        then 1
                        else 0
                    end as proficiente,
                    case
                        when
                            (
                                serie = 5
                                and disciplina = 'LP'
                                and proficiencia_saeb >= 250
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'LP'
                                and proficiencia_saeb >= 325
                            )
                        then 1
                        when
                            (
                                serie = 5
                                and disciplina = 'MT'
                                and proficiencia_saeb >= 275
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'MT'
                                and proficiencia_saeb >= 350
                            )
                        then 1
                        else 0
                    end as avancado,
                    case
                        when
                            (
                                serie = 5
                                and disciplina = 'LP'
                                and proficiencia_saeb >= 200
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'LP'
                                and proficiencia_saeb >= 275
                            )
                        then 1
                        when
                            (
                                serie = 5
                                and disciplina = 'MT'
                                and proficiencia_saeb >= 225
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'MT'
                                and proficiencia_saeb >= 300
                            )
                        then 1
                        else 0
                    end as adequado,
                    case
                        when
                            (
                                serie = 5
                                and disciplina = 'LP'
                                and proficiencia_saeb - 16 < 150
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'LP'
                                and proficiencia_saeb - 16 < 200
                            )
                        then 1
                        when
                            (
                                serie = 5
                                and disciplina = 'MT'
                                and proficiencia_saeb - 20 < 175
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'MT'
                                and proficiencia_saeb - 20 < 225
                            )
                        then 1
                        else 0
                    end as insuficiente_pandemia_pb,
                    case
                        when
                            (
                                serie = 5
                                and disciplina = 'LP'
                                and proficiencia_saeb - 16 >= 150
                                and proficiencia_saeb < 200
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'LP'
                                and proficiencia_saeb - 16 >= 200
                                and proficiencia_saeb < 275
                            )
                        then 1
                        when
                            (
                                serie = 5
                                and disciplina = 'MT'
                                and proficiencia_saeb - 20 >= 175
                                and proficiencia_saeb < 225
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'MT'
                                and proficiencia_saeb - 20 >= 225
                                and proficiencia_saeb < 300
                            )
                        then 1
                        else 0
                    end as basico_pandemia_pb,
                    case
                        when
                            (
                                serie = 5
                                and disciplina = 'LP'
                                and proficiencia_saeb - 16 >= 200
                                and proficiencia_saeb < 250
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'LP'
                                and proficiencia_saeb - 16 >= 275
                                and proficiencia_saeb < 325
                            )
                        then 1
                        when
                            (
                                serie = 5
                                and disciplina = 'MT'
                                and proficiencia_saeb - 20 >= 225
                                and proficiencia_saeb < 275
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'MT'
                                and proficiencia_saeb - 20 >= 300
                                and proficiencia_saeb < 350
                            )
                        then 1
                        else 0
                    end as proficiente_pandemia_pb,
                    case
                        when
                            (
                                serie = 5
                                and disciplina = 'LP'
                                and proficiencia_saeb - 16 >= 250
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'LP'
                                and proficiencia_saeb - 16 >= 325
                            )
                        then 1
                        when
                            (
                                serie = 5
                                and disciplina = 'MT'
                                and proficiencia_saeb - 20 >= 275
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'MT'
                                and proficiencia_saeb - 20 >= 350
                            )
                        then 1
                        else 0
                    end as avancado_pandemia_pb,
                    case
                        when
                            (
                                serie = 5
                                and disciplina = 'LP'
                                and proficiencia_saeb - 16 >= 200
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'LP'
                                and proficiencia_saeb - 16 >= 275
                            )
                        then 1
                        when
                            (
                                serie = 5
                                and disciplina = 'MT'
                                and proficiencia_saeb - 20 >= 225
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'MT'
                                and proficiencia_saeb - 20 >= 300
                            )
                        then 1
                        else 0
                    end as adequado_pandemia_pb,
                    case
                        when
                            (
                                serie = 5
                                and disciplina = 'LP'
                                and proficiencia_saeb - 12 >= 200
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'LP'
                                and proficiencia_saeb - 12 >= 275
                            )
                        then 1
                        when
                            (
                                serie = 5
                                and disciplina = 'MT'
                                and proficiencia_saeb - 14 >= 225
                            )
                        then 1
                        when
                            (
                                serie = 9
                                and disciplina = 'MT'
                                and proficiencia_saeb - 14 >= 300
                            )
                        then 1
                        else 0
                    end as adequado_pandemia_sp
                from {{ set_datalake_project("br_inep_saeb.proficiencia") }}

            ) pivot (
                max(proficiencia_saeb) as proficiencia_saeb,
                max(insuficiente) as insuficiente,
                max(basico) as basico,
                max(proficiente) as proficiente,
                max(avancado) as avancado,
                max(adequado) as adequado,
                max(insuficiente_pandemia_pb) as insuficiente_pandemia_pb,
                max(basico_pandemia_pb) as basico_pandemia_pb,
                max(proficiente_pandemia_pb) as proficiente_pandemia_pb,
                max(avancado_pandemia_pb) as avancado_pandemia_pb,
                max(adequado_pandemia_pb) as adequado_pandemia_pb,
                max(adequado_pandemia_sp) as adequado_pandemia_sp
                for disciplina in ('LP', 'MT')
            )
    )

select *
from
    (
        select
            ano,
            id_escola,
            serie,

            sum(proficiencia_saeb_lp * peso_aluno) / sum(peso_aluno) as proficiencia_lp,
            sum(proficiencia_saeb_mt * peso_aluno) / sum(peso_aluno) as proficiencia_mt,

            100
            * sum(insuficiente_lp * peso_aluno)
            / sum(peso_aluno) as insuficiente_lp,
            100
            * sum(insuficiente_mt * peso_aluno)
            / sum(peso_aluno) as insuficiente_mt,
            100 * sum(basico_lp * peso_aluno) / sum(peso_aluno) as basico_lp,
            100 * sum(basico_mt * peso_aluno) / sum(peso_aluno) as basico_mt,
            100 * sum(proficiente_lp * peso_aluno) / sum(peso_aluno) as proficiente_lp,
            100 * sum(proficiente_mt * peso_aluno) / sum(peso_aluno) as proficiente_mt,
            100 * sum(avancado_lp * peso_aluno) / sum(peso_aluno) as avancado_lp,
            100 * sum(avancado_mt * peso_aluno) / sum(peso_aluno) as avancado_mt,
            100 * sum(adequado_lp * peso_aluno) / sum(peso_aluno) as adequado_lp,
            100 * sum(adequado_mt * peso_aluno) / sum(peso_aluno) as adequado_mt,

            100
            * sum(insuficiente_pandemia_pb_lp * peso_aluno)
            / sum(peso_aluno) as insuficiente_pandemia_pb_lp,
            100
            * sum(insuficiente_pandemia_pb_mt * peso_aluno)
            / sum(peso_aluno) as insuficiente_pandemia_pb_mt,
            100
            * sum(basico_pandemia_pb_lp * peso_aluno)
            / sum(peso_aluno) as basico_pandemia_pb_lp,
            100
            * sum(basico_pandemia_pb_mt * peso_aluno)
            / sum(peso_aluno) as basico_pandemia_pb_mt,
            100
            * sum(proficiente_pandemia_pb_lp * peso_aluno)
            / sum(peso_aluno) as proficiente_pandemia_pb_lp,
            100
            * sum(proficiente_pandemia_pb_mt * peso_aluno)
            / sum(peso_aluno) as proficiente_pandemia_pb_mt,
            100
            * sum(avancado_pandemia_pb_lp * peso_aluno)
            / sum(peso_aluno) as avancado_pandemia_pb_lp,
            100
            * sum(avancado_pandemia_pb_mt * peso_aluno)
            / sum(peso_aluno) as avancado_pandemia_pb_mt,
            100
            * sum(adequado_pandemia_pb_lp * peso_aluno)
            / sum(peso_aluno) as adequado_pandemia_pb_lp,
            100
            * sum(adequado_pandemia_pb_mt * peso_aluno)
            / sum(peso_aluno) as adequado_pandemia_pb_mt,
            100
            * sum(adequado_pandemia_sp_lp * peso_aluno)
            / sum(peso_aluno) as adequado_pandemia_sp_lp,
            100
            * sum(adequado_pandemia_sp_mt * peso_aluno)
            / sum(peso_aluno) as adequado_pandemia_sp_mt,

        from saeb_pivot
        group by ano, id_escola, serie
        order by ano, id_escola, serie asc
    )
order by id_escola, serie, ano
