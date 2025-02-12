# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

# pylint: disable=invalid-name, too-many-lines
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for utils.
    """

    COLUMNS_ORDER = [
        "id_uf",
        "capital",
        "rm_ride",
        "id_upa",
        "id_estrato",
        "id_domicilio",
        "V1008",
        "V1014",
        "V1016",
        "V1022",
        "V1023",
        "V1027",
        "V1028",
        "V1029",
        "V1033",
        "posest",
        "posest_sxi",
        "V2001",
        "V2003",
        "V2005",
        "V2007",
        "V2008",
        "V20081",
        "V20082",
        "V2009",
        "V2010",
        "V3001",
        "V3002",
        "V3002A",
        "V3003",
        "V3003A",
        "V3004",
        "V3005",
        "V3005A",
        "V3006",
        "V3006A",
        "V3007",
        "V3008",
        "V3009",
        "V3009A",
        "V3010",
        "V3011",
        "V3011A",
        "V3012",
        "V3013",
        "V3013A",
        "V3013B",
        "V3014",
        "V4001",
        "V4002",
        "V4003",
        "V4004",
        "V4005",
        "V4006",
        "V4006A",
        "V4007",
        "V4008",
        "V40081",
        "V40082",
        "V40083",
        "V4009",
        "V4010",
        "V4012",
        "V40121",
        "V4013",
        "V40132",
        "V40132A",
        "V4014",
        "V4015",
        "V40151",
        "V401511",
        "V401512",
        "V4016",
        "V40161",
        "V40162",
        "V40163",
        "V4017",
        "V40171",
        "V401711",
        "V4018",
        "V40181",
        "V40182",
        "V40183",
        "V4019",
        "V4020",
        "V4021",
        "V4022",
        "V4024",
        "V4025",
        "V4026",
        "V4027",
        "V4028",
        "V4029",
        "V4032",
        "V4033",
        "V40331",
        "V403311",
        "V403312",
        "V40332",
        "V403321",
        "V403322",
        "V40333",
        "V403331",
        "V4034",
        "V40341",
        "V403411",
        "V403412",
        "V40342",
        "V403421",
        "V403422",
        "V4039",
        "V4039C",
        "V4040",
        "V40401",
        "V40402",
        "V40403",
        "V4041",
        "V4043",
        "V40431",
        "V4044",
        "V4045",
        "V4046",
        "V4047",
        "V4048",
        "V4049",
        "V4050",
        "V40501",
        "V405011",
        "V405012",
        "V40502",
        "V405021",
        "V405022",
        "V40503",
        "V405031",
        "V4051",
        "V40511",
        "V405111",
        "V405112",
        "V40512",
        "V405121",
        "V405122",
        "V4056",
        "V4056C",
        "V4057",
        "V4058",
        "V40581",
        "V405811",
        "V405812",
        "V40582",
        "V405821",
        "V405822",
        "V40583",
        "V405831",
        "V40584",
        "V4059",
        "V40591",
        "V405911",
        "V405912",
        "V40592",
        "V405921",
        "V405922",
        "V4062",
        "V4062C",
        "V4063",
        "V4063A",
        "V4064",
        "V4064A",
        "V4071",
        "V4072",
        "V4072A",
        "V4073",
        "V4074",
        "V4074A",
        "V4075A",
        "V4075A1",
        "V4076",
        "V40761",
        "V40762",
        "V40763",
        "V4077",
        "V4078",
        "V4078A",
        "V4082",
        "VD2002",
        "VD2003",
        "VD2004",
        "VD3004",
        "VD3005",
        "VD3006",
        "VD4001",
        "VD4002",
        "VD4003",
        "VD4004",
        "VD4004A",
        "VD4005",
        "VD4007",
        "VD4008",
        "VD4009",
        "VD4010",
        "VD4011",
        "VD4012",
        "VD4013",
        "VD4014",
        "VD4015",
        "VD4016",
        "VD4017",
        "VD4018",
        "VD4019",
        "VD4020",
        "VD4023",
        "VD4030",
        "VD4031",
        "VD4032",
        "VD4033",
        "VD4034",
        "VD4035",
        "VD4036",
        "VD4037",
        "V1028001",
        "V1028002",
        "V1028003",
        "V1028004",
        "V1028005",
        "V1028006",
        "V1028007",
        "V1028008",
        "V1028009",
        "V1028010",
        "V1028011",
        "V1028012",
        "V1028013",
        "V1028014",
        "V1028015",
        "V1028016",
        "V1028017",
        "V1028018",
        "V1028019",
        "V1028020",
        "V1028021",
        "V1028022",
        "V1028023",
        "V1028024",
        "V1028025",
        "V1028026",
        "V1028027",
        "V1028028",
        "V1028029",
        "V1028030",
        "V1028031",
        "V1028032",
        "V1028033",
        "V1028034",
        "V1028035",
        "V1028036",
        "V1028037",
        "V1028038",
        "V1028039",
        "V1028040",
        "V1028041",
        "V1028042",
        "V1028043",
        "V1028044",
        "V1028045",
        "V1028046",
        "V1028047",
        "V1028048",
        "V1028049",
        "V1028050",
        "V1028051",
        "V1028052",
        "V1028053",
        "V1028054",
        "V1028055",
        "V1028056",
        "V1028057",
        "V1028058",
        "V1028059",
        "V1028060",
        "V1028061",
        "V1028062",
        "V1028063",
        "V1028064",
        "V1028065",
        "V1028066",
        "V1028067",
        "V1028068",
        "V1028069",
        "V1028070",
        "V1028071",
        "V1028072",
        "V1028073",
        "V1028074",
        "V1028075",
        "V1028076",
        "V1028077",
        "V1028078",
        "V1028079",
        "V1028080",
        "V1028081",
        "V1028082",
        "V1028083",
        "V1028084",
        "V1028085",
        "V1028086",
        "V1028087",
        "V1028088",
        "V1028089",
        "V1028090",
        "V1028091",
        "V1028092",
        "V1028093",
        "V1028094",
        "V1028095",
        "V1028096",
        "V1028097",
        "V1028098",
        "V1028099",
        "V1028100",
        "V1028101",
        "V1028102",
        "V1028103",
        "V1028104",
        "V1028105",
        "V1028106",
        "V1028107",
        "V1028108",
        "V1028109",
        "V1028110",
        "V1028111",
        "V1028112",
        "V1028113",
        "V1028114",
        "V1028115",
        "V1028116",
        "V1028117",
        "V1028118",
        "V1028119",
        "V1028120",
        "V1028121",
        "V1028122",
        "V1028123",
        "V1028124",
        "V1028125",
        "V1028126",
        "V1028127",
        "V1028128",
        "V1028129",
        "V1028130",
        "V1028131",
        "V1028132",
        "V1028133",
        "V1028134",
        "V1028135",
        "V1028136",
        "V1028137",
        "V1028138",
        "V1028139",
        "V1028140",
        "V1028141",
        "V1028142",
        "V1028143",
        "V1028144",
        "V1028145",
        "V1028146",
        "V1028147",
        "V1028148",
        "V1028149",
        "V1028150",
        "V1028151",
        "V1028152",
        "V1028153",
        "V1028154",
        "V1028155",
        "V1028156",
        "V1028157",
        "V1028158",
        "V1028159",
        "V1028160",
        "V1028161",
        "V1028162",
        "V1028163",
        "V1028164",
        "V1028165",
        "V1028166",
        "V1028167",
        "V1028168",
        "V1028169",
        "V1028170",
        "V1028171",
        "V1028172",
        "V1028173",
        "V1028174",
        "V1028175",
        "V1028176",
        "V1028177",
        "V1028178",
        "V1028179",
        "V1028180",
        "V1028181",
        "V1028182",
        "V1028183",
        "V1028184",
        "V1028185",
        "V1028186",
        "V1028187",
        "V1028188",
        "V1028189",
        "V1028190",
        "V1028191",
        "V1028192",
        "V1028193",
        "V1028194",
        "V1028195",
        "V1028196",
        "V1028197",
        "V1028198",
        "V1028199",
        "V1028200",
        "habitual",
        "efetivo",
        "ano",
        "trimestre",
        "sigla_uf",
    ]

    map_codigo_sigla_uf = {
        "11": "RO",
        "12": "AC",
        "13": "AM",
        "14": "RR",
        "15": "PA",
        "16": "AP",
        "17": "TO",
        "21": "MA",
        "22": "PI",
        "23": "CE",
        "24": "RN",
        "25": "PB",
        "26": "PE",
        "27": "AL",
        "28": "SE",
        "29": "BA",
        "31": "MG",
        "32": "ES",
        "33": "RJ",
        "35": "SP",
        "41": "PR",
        "42": "SC",
        "43": "RS",
        "50": "MS",
        "51": "MT",
        "52": "GO",
        "53": "DF",
    }

    URL_PREFIX = "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/{year}/"
    COLUMNS_WIDTHS = [
        4,
        1,
        2,
        2,
        2,
        9,
        7,
        2,
        2,
        1,
        1,
        1,
        15,
        15,
        9,
        9,
        3,
        3,
        2,
        2,
        2,
        1,
        2,
        2,
        4,
        3,
        1,
        1,
        1,
        1,
        2,
        2,
        1,
        1,
        1,
        2,
        1,
        1,
        1,
        2,
        2,
        1,
        1,
        1,
        1,
        2,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        2,
        2,
        1,
        4,
        1,
        1,
        5,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        1,
        1,
        2,
        2,
        1,
        1,
        1,
        1,
        1,
        2,
        2,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        8,
        1,
        1,
        8,
        1,
        1,
        1,
        1,
        1,
        8,
        1,
        1,
        8,
        3,
        3,
        1,
        2,
        2,
        2,
        4,
        1,
        1,
        5,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        8,
        1,
        1,
        8,
        1,
        1,
        1,
        1,
        1,
        8,
        1,
        1,
        8,
        3,
        3,
        1,
        1,
        1,
        1,
        8,
        1,
        1,
        8,
        1,
        1,
        1,
        1,
        1,
        1,
        8,
        1,
        1,
        8,
        3,
        3,
        1,
        1,
        1,
        1,
        1,
        2,
        1,
        1,
        1,
        2,
        1,
        2,
        1,
        2,
        2,
        2,
        1,
        1,
        1,
        1,
        2,
        2,
        1,
        2,
        1,
        2,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        2,
        2,
        1,
        1,
        1,
        1,
        8,
        8,
        1,
        8,
        8,
        1,
        1,
        3,
        3,
        3,
        3,
        3,
        1,
        1,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
        15,
    ]
    COLUMNS_NAMES = [
        "Ano",
        "Trimestre",
        "UF",
        "Capital",
        "RM_RIDE",
        "UPA",
        "Estrato",
        "V1008",
        "V1014",
        "V1016",
        "V1022",
        "V1023",
        "V1027",
        "V1028",
        "V1029",
        "V1033",
        "posest",
        "posest_sxi",
        "V2001",
        "V2003",
        "V2005",
        "V2007",
        "V2008",
        "V20081",
        "V20082",
        "V2009",
        "V2010",
        "V3001",
        "V3002",
        "V3002A",
        "V3003",
        "V3003A",
        "V3004",
        "V3005",
        "V3005A",
        "V3006",
        "V3006A",
        "V3007",
        "V3008",
        "V3009",
        "V3009A",
        "V3010",
        "V3011",
        "V3011A",
        "V3012",
        "V3013",
        "V3013A",
        "V3013B",
        "V3014",
        "V4001",
        "V4002",
        "V4003",
        "V4004",
        "V4005",
        "V4006",
        "V4006A",
        "V4007",
        "V4008",
        "V40081",
        "V40082",
        "V40083",
        "V4009",
        "V4010",
        "V4012",
        "V40121",
        "V4013",
        "V40132",
        "V40132A",
        "V4014",
        "V4015",
        "V40151",
        "V401511",
        "V401512",
        "V4016",
        "V40161",
        "V40162",
        "V40163",
        "V4017",
        "V40171",
        "V401711",
        "V4018",
        "V40181",
        "V40182",
        "V40183",
        "V4019",
        "V4020",
        "V4021",
        "V4022",
        "V4024",
        "V4025",
        "V4026",
        "V4027",
        "V4028",
        "V4029",
        "V4032",
        "V4033",
        "V40331",
        "V403311",
        "V403312",
        "V40332",
        "V403321",
        "V403322",
        "V40333",
        "V403331",
        "V4034",
        "V40341",
        "V403411",
        "V403412",
        "V40342",
        "V403421",
        "V403422",
        "V4039",
        "V4039C",
        "V4040",
        "V40401",
        "V40402",
        "V40403",
        "V4041",
        "V4043",
        "V40431",
        "V4044",
        "V4045",
        "V4046",
        "V4047",
        "V4048",
        "V4049",
        "V4050",
        "V40501",
        "V405011",
        "V405012",
        "V40502",
        "V405021",
        "V405022",
        "V40503",
        "V405031",
        "V4051",
        "V40511",
        "V405111",
        "V405112",
        "V40512",
        "V405121",
        "V405122",
        "V4056",
        "V4056C",
        "V4057",
        "V4058",
        "V40581",
        "V405811",
        "V405812",
        "V40582",
        "V405821",
        "V405822",
        "V40583",
        "V405831",
        "V40584",
        "V4059",
        "V40591",
        "V405911",
        "V405912",
        "V40592",
        "V405921",
        "V405922",
        "V4062",
        "V4062C",
        "V4063",
        "V4063A",
        "V4064",
        "V4064A",
        "V4071",
        "V4072",
        "V4072A",
        "V4073",
        "V4074",
        "V4074A",
        "V4075A",
        "V4075A1",
        "V4076",
        "V40761",
        "V40762",
        "V40763",
        "V4077",
        "V4078",
        "V4078A",
        "V4082",
        "VD2002",
        "VD2003",
        "VD2004",
        "VD2006",
        "VD3004",
        "VD3005",
        "VD3006",
        "VD4001",
        "VD4002",
        "VD4003",
        "VD4004",
        "VD4004A",
        "VD4005",
        "VD4007",
        "VD4008",
        "VD4009",
        "VD4010",
        "VD4011",
        "VD4012",
        "VD4013",
        "VD4014",
        "VD4015",
        "VD4016",
        "VD4017",
        "VD4018",
        "VD4019",
        "VD4020",
        "VD4023",
        "VD4030",
        "VD4031",
        "VD4032",
        "VD4033",
        "VD4034",
        "VD4035",
        "VD4036",
        "VD4037",
        "V1028001",
        "V1028002",
        "V1028003",
        "V1028004",
        "V1028005",
        "V1028006",
        "V1028007",
        "V1028008",
        "V1028009",
        "V1028010",
        "V1028011",
        "V1028012",
        "V1028013",
        "V1028014",
        "V1028015",
        "V1028016",
        "V1028017",
        "V1028018",
        "V1028019",
        "V1028020",
        "V1028021",
        "V1028022",
        "V1028023",
        "V1028024",
        "V1028025",
        "V1028026",
        "V1028027",
        "V1028028",
        "V1028029",
        "V1028030",
        "V1028031",
        "V1028032",
        "V1028033",
        "V1028034",
        "V1028035",
        "V1028036",
        "V1028037",
        "V1028038",
        "V1028039",
        "V1028040",
        "V1028041",
        "V1028042",
        "V1028043",
        "V1028044",
        "V1028045",
        "V1028046",
        "V1028047",
        "V1028048",
        "V1028049",
        "V1028050",
        "V1028051",
        "V1028052",
        "V1028053",
        "V1028054",
        "V1028055",
        "V1028056",
        "V1028057",
        "V1028058",
        "V1028059",
        "V1028060",
        "V1028061",
        "V1028062",
        "V1028063",
        "V1028064",
        "V1028065",
        "V1028066",
        "V1028067",
        "V1028068",
        "V1028069",
        "V1028070",
        "V1028071",
        "V1028072",
        "V1028073",
        "V1028074",
        "V1028075",
        "V1028076",
        "V1028077",
        "V1028078",
        "V1028079",
        "V1028080",
        "V1028081",
        "V1028082",
        "V1028083",
        "V1028084",
        "V1028085",
        "V1028086",
        "V1028087",
        "V1028088",
        "V1028089",
        "V1028090",
        "V1028091",
        "V1028092",
        "V1028093",
        "V1028094",
        "V1028095",
        "V1028096",
        "V1028097",
        "V1028098",
        "V1028099",
        "V1028100",
        "V1028101",
        "V1028102",
        "V1028103",
        "V1028104",
        "V1028105",
        "V1028106",
        "V1028107",
        "V1028108",
        "V1028109",
        "V1028110",
        "V1028111",
        "V1028112",
        "V1028113",
        "V1028114",
        "V1028115",
        "V1028116",
        "V1028117",
        "V1028118",
        "V1028119",
        "V1028120",
        "V1028121",
        "V1028122",
        "V1028123",
        "V1028124",
        "V1028125",
        "V1028126",
        "V1028127",
        "V1028128",
        "V1028129",
        "V1028130",
        "V1028131",
        "V1028132",
        "V1028133",
        "V1028134",
        "V1028135",
        "V1028136",
        "V1028137",
        "V1028138",
        "V1028139",
        "V1028140",
        "V1028141",
        "V1028142",
        "V1028143",
        "V1028144",
        "V1028145",
        "V1028146",
        "V1028147",
        "V1028148",
        "V1028149",
        "V1028150",
        "V1028151",
        "V1028152",
        "V1028153",
        "V1028154",
        "V1028155",
        "V1028156",
        "V1028157",
        "V1028158",
        "V1028159",
        "V1028160",
        "V1028161",
        "V1028162",
        "V1028163",
        "V1028164",
        "V1028165",
        "V1028166",
        "V1028167",
        "V1028168",
        "V1028169",
        "V1028170",
        "V1028171",
        "V1028172",
        "V1028173",
        "V1028174",
        "V1028175",
        "V1028176",
        "V1028177",
        "V1028178",
        "V1028179",
        "V1028180",
        "V1028181",
        "V1028182",
        "V1028183",
        "V1028184",
        "V1028185",
        "V1028186",
        "V1028187",
        "V1028188",
        "V1028189",
        "V1028190",
        "V1028191",
        "V1028192",
        "V1028193",
        "V1028194",
        "V1028195",
        "V1028196",
        "V1028197",
        "V1028198",
        "V1028199",
        "V1028200",
    ]
