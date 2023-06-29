# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the mercadolivre_ofertas project
    """

    LESS100 = "https://www.mercadolivre.com.br/ofertas?container_id=MLB779362-1&price=0.0-100.0#origin=scut&filter_applied=price&filter_position=6&is_recommended_domain=false"
    OFERTA_DIA = "https://www.mercadolivre.com.br/ofertas#nav-header"
    RELAMPAGO = "https://www.mercadolivre.com.br/ofertas?promotion_type=lightning&container_id=MLB779362-1#origin=scut&filter_applied=promotion_type&filter_position=2&is_recommended_domain=false"
    BARATO_DIA = "https://www.mercadolivre.com.br/ofertas?deal_ids=MLB861109-2&container_id=MLB861109-2#origin=scut&filter_applied=deal_ids&filter_position=3&is_recommended_domain=false"
    KWARGS_LIST = [
        {"class_": "ui-pdp-title"},
        {"class_": "ui-pdp-review__amount"},
        {"class_": "andes-money-amount__discount"},
        {
            "class_": "ui-pdp-color--BLACK ui-pdp-family--REGULAR ui-pdp-media__title ui-pdp-media__title--on-hover"
        },
        {
            "class_": "ui-review-capability__rating__average ui-review-capability__rating__average--desktop"
        },
    ]
    TABLES_NAMES = ["less100", "oferta_dia", "relampago", "barato_dia"]
