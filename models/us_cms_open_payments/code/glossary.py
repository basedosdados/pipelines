"""Portuguese gloss for every categorical value that reaches the dicionario.

Open Payments publishes readable English labels rather than codes, so the
dictionary's job is translation: ``chave`` is the value exactly as CMS wrote
it, ``valor`` is what it means in Portuguese. gen_dicionario fails loudly on
any value missing from this map, so a new value in a future program year
cannot slip through unglossed.
"""

GLOSS = {
    # change_type
    "NEW": "Registro novo, incluído desde o prazo de submissão anterior",
    "ADD": "Registro que não era elegível na publicação anterior e passou a ser",
    "CHANGED": "Registro publicado anteriormente e alterado depois da última publicação",
    "UNCHANGED": "Registro publicado anteriormente e inalterado nesta publicação",
    # yes/no indicators
    "Yes": "Sim",
    "No": "Não",
    "true": "Sim",
    "false": "Não",
    # covered_recipient_type / profile_type / recipient_type
    "Covered Recipient Physician": "Médico beneficiário",
    "Covered Recipient Non-Physician Practitioner": "Profissional não médico beneficiário",
    "Covered Recipient Physician/Covered Recipient Non-Physician Practitioner": (
        "Perfil que consta como médico e como profissional não médico"
    ),
    "Covered Recipient Teaching Hospital": "Hospital universitário beneficiário",
    "Non-covered Recipient Entity": "Entidade não coberta pelo programa",
    "Non-covered Recipient Individual": "Pessoa física não coberta pelo programa",
    "Teaching Hospital": "Hospital universitário",
    "Physician": "Médico",
    "Non-Physician Practitioner": "Profissional não médico",
    # payment_form
    "Cash or cash equivalent": "Dinheiro ou equivalente de caixa",
    "In-kind items and services": "Bens e serviços em espécie",
    "Stock, stock option, or any other ownership interest": (
        "Ações, opções de ações ou qualquer outra participação societária"
    ),
    "Dividend, profit or other return on investment": (
        "Dividendos, lucros ou outro retorno sobre investimento"
    ),
    # payment_nature
    "Charitable Contribution": "Contribuição para instituição de caridade",
    "Compensation for services other than consulting, including serving as faculty or as a "
    "speaker at a venue other than a continuing education program": (
        "Remuneração por serviços que não consultoria, incluindo atuação como docente ou "
        "palestrante fora de programa de educação continuada"
    ),
    "Compensation for serving as faculty or as a speaker for a non-accredited and noncertified "
    "continuing education program": (
        "Remuneração por atuação como docente ou palestrante em programa de educação continuada "
        "não credenciado e não certificado"
    ),
    "Compensation for serving as faculty or as a speaker for an accredited or certified "
    "continuing education program": (
        "Remuneração por atuação como docente ou palestrante em programa de educação continuada "
        "credenciado ou certificado"
    ),
    "Compensation for serving as faculty or as a speaker for a medical education program": (
        "Remuneração por atuação como docente ou palestrante em programa de educação médica"
    ),
    "Consulting Fee": "Honorários de consultoria",
    "Current or prospective ownership or investment interest": (
        "Participação societária ou de investimento atual ou futura"
    ),
    "Education": "Educação",
    "Entertainment": "Entretenimento",
    "Food and Beverage": "Alimentação e bebidas",
    "Gift": "Presente",
    "Grant": "Subvenção",
    "Honoraria": "Honorários",
    "Royalty or License": "Royalties ou licenciamento",
    "Space rental or facility fees (teaching hospital only)": (
        "Aluguel de espaço ou taxas de instalação, apenas para hospitais universitários"
    ),
    "Travel and Lodging": "Viagem e hospedagem",
    "Acquisitions": "Aquisições",
    "Debt forgiveness": "Perdão de dívida",
    "Long term medical supply or device loan": (
        "Empréstimo de longo prazo de insumo ou dispositivo médico"
    ),
    # payment_type
    "General": "Pagamento geral",
    "Research": "Pagamento de pesquisa",
    "Ownership": "Participação societária",
    # related_product_indicator
    "Covered": "Associado a produto coberto pelo programa",
    "Non-Covered": "Associado a produto não coberto pelo programa",
    "Combination": "Associado a produtos cobertos e não cobertos",
    "None": "Não associado a nenhum produto",
    # product_type
    "Drug": "Medicamento",
    "Biological": "Produto biológico",
    "Device": "Dispositivo médico",
    "Medical Supply": "Insumo médico",
    # primary_type
    "Medical Doctor": "Médico",
    "Doctor of Osteopathy": "Médico osteopata",
    "Doctor of Dentistry": "Cirurgião-dentista",
    "Doctor of Optometry": "Optometrista",
    "Doctor of Podiatric Medicine": "Podólogo",
    "Chiropractor": "Quiroprata",
    "Nurse Practitioner": "Enfermeiro de prática avançada",
    "Physician Assistant": "Assistente médico",
    "Certified Registered Nurse Anesthetist": "Enfermeiro anestesista certificado",
    "Clinical Nurse Specialist": "Enfermeiro clínico especialista",
    "Certified Nurse-Midwife": "Enfermeiro obstetra certificado",
    "Anesthesiologist Assistant": "Assistente de anestesiologia",
    # third_party_payment_recipient_indicator
    "Entity": "Pagamento feito a uma entidade terceira",
    "Individual": "Pagamento feito a uma pessoa física terceira",
    "No Third Party Payment": "Sem pagamento a terceiros",
    # interest_held_by_physician_or_family
    "Physician Covered Recipient": "Participação detida pelo próprio médico",
    "Immediate family member": "Participação detida por familiar imediato",
    # expenditure_category
    "Patient Care": "Assistência ao paciente",
    "Non-patient Care": "Atividades que não assistência ao paciente",
    "Overhead": "Custos indiretos",
    "Professional Salary Support": "Apoio salarial a profissionais",
    "Medical Research Writing or Publication": "Redação ou publicação de pesquisa médica",
    "Other": "Outros",
    # metric_level
    "National": "Âmbito nacional",
    "State": "Âmbito estadual",
}
