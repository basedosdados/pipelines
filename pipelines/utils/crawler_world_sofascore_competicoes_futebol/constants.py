# -*- coding: utf-8 -*-
"""
Constants for br_tse_eleicoes pipeline.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for utils.
    """

    MODE_TO_PROJECT_DICT = {
        "uefa_champions_league": "https://www.sofascore.com/pt/torneio/futebol/europe/uefa-champions-league/7",
        "brasileirao_serie_a": "https://www.sofascore.com/pt/torneio/futebol/brazil/brasileirao-serie-a/325",
    }

    QUERY_COUNT_MODIFIED = """SELECT
  (SELECT count(*) as total FROM `{mode}.br_tse_eleicoes.{table_id}` WHERE ano={year}) AS total,
  (SELECT TIMESTAMP_MILLIS(creation_time) as last_modified_time
   FROM `{mode}.br_tse_eleicoes.__TABLES_SUMMARY__`
   WHERE table_id = '{table_id}') AS last_modified_time;"""

    TRANSLETE = {
        "qualification round 1": "Primeiro Rodada De Qualificação",
        "qualification round 2": "Segunda Rodada De Qualificação",
        "qualification round 3": "Terceira Rodada De Qualificação",
        "qualification round 4": "Quarta Rodada De Qualificação",
        "playoff round": "Rodada De Playoff",
        "round of 16": "Oitavos-de-final",
        "quarterfinals": "Quartas De Final",
        "semifinals": "Semifinal",
        "final": "Final",
    }

    COLUNAS_DICT_ORDEM = {
        "ano": "ano",
        "id_partida": "id_partida",
        "data": "data",
        "hora": "hora",
        "temporada": "temporada",
        "rodada": "rodada",
        "tempo": "tempo",
        "mandante_name": "time_mandante",
        "visitante_name": "time_visitante",
        "mandante_score": "gols_mandante",
        "visitante_score": "gols_visitante",
        "mandante_ballPossession": "posse_bola_mandante",
        "visitante_ballPossession": "posse_bola_visitante",
        "mandante_expectedGoals": "gol_esperado_mandante",
        "visitante_expectedGoals": "gol_esperado_visitante",
        "mandante_bigChanceCreated": "grande_chance_mandante",
        "visitante_bigChanceCreated": "grande_chance_visitante",
        "mandante_goalkeeperSaves": "defesa_goleiro_mandante",
        "visitante_goalkeeperSaves": "defesa_goleiro_visitante",
        "mandante_cornerKicks": "escanteio_mandante",
        "visitante_cornerKicks": "escanteio_visitante",
        "mandante_passes": "passe_mandante",
        "visitante_passes": "passe_visitante",
        "mandante_freeKicks": "falta_mandante",
        "visitante_freeKicks": "falta_visitante",
        "mandante_yellowCards": "cartao_amarelo_mandante",
        "visitante_yellowCards": "cartao_amarelo_visitante",
        "mandante_redCards": "cartao_vermelho_mandante",
        "visitante_redCards": "cartao_vermelho_visitante",
        "mandante_totalShotsOnGoal": "finalizacao_mandante",
        "visitante_totalShotsOnGoal": "finalizacao_visitante",
        "mandante_shotsOnGoal": "finalizacao_gol_mandante",
        "visitante_shotsOnGoal": "finalizacao_gol_visitante",
        "mandante_hitWoodwork": "finalizacao_trave_mandante",
        "visitante_hitWoodwork": "finalizacao_trave_visitante",
        "mandante_shotsOffGoal": "finalizacao_fora_mandante",
        "visitante_shotsOffGoal": "finalizacao_fora_visitante",
        "mandante_blockedScoringAttempt": "chute_bloqueado_mandante",
        "visitante_blockedScoringAttempt": "chute_bloqueado_visitante",
        "mandante_totalShotsInsideBox": "finalizacao_area_mandante",
        "visitante_totalShotsInsideBox": "finalizacao_area_visitante",
        "mandante_totalShotsOutsideBox": "finalizacao_fora_area_mandante",
        "visitante_totalShotsOutsideBox": "finalizacao_fora_area_visitante",
        "mandante_bigChanceScored": "grande_chance_marcada_mandante",
        "visitante_bigChanceScored": "grande_chance_marcada_visitante",
        "mandante_bigChanceMissed": "grande_chance_perdida_mandante",
        "visitante_bigChanceMissed": "grande_chance_perdida_visitante",
        "mandante_accurateThroughBall": "passe_profundidade_mandante",
        "visitante_accurateThroughBall": "passe_profundidade_visitante",
        "mandante_fouledFinalThird": "falta_terco_final_mandante",
        "visitante_fouledFinalThird": "falta_terco_final_visitante",
        "mandante_offsides": "impedimento_mandante",
        "visitante_offsides": "impedimento_visitante",
        "mandante_accuratePasses": "passes_certo_mandante",
        "visitante_accuratePasses": "passes_certo_visitante",
        "mandante_throwIns": "lateral_mandante",
        "visitante_throwIns": "lateral_visitante",
        "mandante_finalThirdEntries": "entrada_terco_final_mandante",
        "visitante_finalThirdEntries": "entrada_terco_final_visitante",
        "mandante_accurateLongBalls": "bola_longa_mandante",
        "visitante_accurateLongBalls": "bola_longa_visitante",
        "mandante_accurateCross": "cruzamento_mandante",
        "visitante_accurateCross": "cruzamento_visitante",
        "mandante_duelWonPercent": "duelo_vencido_mandante",
        "visitante_duelWonPercent": "duelo_vencido_visitante",
        "mandante_dispossessed": "desarme_sofrido_mandante",
        "visitante_dispossessed": "desarme_sofrido_visitante",
        "mandante_groundDuelsPercentage": "duelo_chao_mandante",
        "visitante_groundDuelsPercentage": "duelo_chao_visitante",
        "mandante_aerialDuelsPercentage": "duelo_aereo_mandante",
        "visitante_aerialDuelsPercentage": "duelo_aereo_visitante",
        "mandante_dribblesPercentage": "drible_mandante",
        "visitante_dribblesPercentage": "drible_visitante",
        "mandante_totalTackle": "desarme_mandante",
        "visitante_totalTackle": "desarme_visitante",
        "mandante_wonTacklePercent": "desarme_ganho_mandante",
        "visitante_wonTacklePercent": "desarme_ganho_visitante",
        "mandante_interceptionWon": "interceptacao_mandante",
        "visitante_interceptionWon": "interceptacao_visitante",
        "mandante_ballRecovery": "recuperacao_bola_mandante",
        "visitante_ballRecovery": "recuperacao_bola_visitante",
        "mandante_totalClearance": "corte_mandante",
        "visitante_totalClearance": "corte_visitante",
        "mandante_goalKicks": "tiro_meta_mandante",
        "visitante_goalKicks": "tiro_meta_visitante",
    }
