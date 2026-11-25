"""Explicit PT/ES renderings of the financial line-item descriptions.

The FDIC publishes these as heavily abbreviated capitalised titles ("N/C Const
Real Estate/Const Real Estate", "Carry Amt Loss Share-Lnls"), which no word-level
glossary can render with correct word order or grammar -- Portuguese and Spanish
put the qualifier after the head noun where English puts it before.

Keyed by the English description exactly as it appears in architecture/financials.csv.
"""

FINANCIALS: dict[str, tuple[str, str]] = {
    # --- definitions the FDIC writes out in full ---
    "Quarterly net interest income plus total noninterest income plus realized gains (losses) on securities and extraordinary items, less total noninterest expense, loan loss provisions and income taxes.": (
        "Receita líquida de juros do trimestre mais receitas não decorrentes de juros mais ganhos (perdas) realizados com títulos e itens extraordinários, menos despesas não decorrentes de juros, provisões para perdas com empréstimos e impostos sobre a renda",
        "Ingreso neto por intereses del trimestre más ingresos no derivados de intereses más ganancias (pérdidas) realizadas en valores y partidas extraordinarias, menos gastos no derivados de intereses, provisiones para pérdidas por préstamos e impuestos sobre la renta",
    ),
    "Net interest income plus total noninterest income plus realized gains (losses) on securities and extraordinary items, less total noninterest expense, loan loss provisions and income taxes.": (
        "Receita líquida de juros mais receitas não decorrentes de juros mais ganhos (perdas) realizados com títulos e itens extraordinários, menos despesas não decorrentes de juros, provisões para perdas com empréstimos e impostos sobre a renda",
        "Ingreso neto por intereses más ingresos no derivados de intereses más ganancias (pérdidas) realizadas en valores y partidas extraordinarias, menos gastos no derivados de intereses, provisiones para pérdidas por préstamos e impuestos sobre la renta",
    ),
    "Total equity capital (includes preferred and common stock, surplus and undivided profits).": (
        "Patrimônio líquido total (inclui ações preferenciais e ordinárias, reservas de capital e lucros acumulados)",
        "Patrimonio neto total (incluye acciones preferentes y ordinarias, reservas de capital y utilidades acumuladas)",
    ),
    "The sum of all assets owned by the institution including cash, loans, securities, bank premises and other assets. This total does not include off-balance-sheet accounts.": (
        "Soma de todos os ativos da instituição, incluindo caixa, empréstimos, títulos, imóveis e demais ativos. Não inclui contas fora do balanço",
        "Suma de todos los activos de la institución, incluyendo efectivo, préstamos, valores, inmuebles y demás activos. No incluye cuentas fuera de balance",
    ),
    "The sum of all deposits including demand deposits, money market deposits, other savings deposits, time deposits and deposits in foreign offices.": (
        "Soma de todos os depósitos, incluindo depósitos à vista, depósitos de mercado monetário, demais depósitos de poupança, depósitos a prazo e depósitos em agências no exterior",
        "Suma de todos los depósitos, incluyendo depósitos a la vista, depósitos de mercado monetario, demás depósitos de ahorro, depósitos a plazo y depósitos en oficinas del exterior",
    ),
    "The sum of all domestic office deposits, including demand deposits, money market deposits, other savings deposits and time deposits.": (
        "Soma de todos os depósitos em agências domésticas, incluindo depósitos à vista, depósitos de mercado monetário, demais depósitos de poupança e depósitos a prazo",
        "Suma de todos los depósitos en oficinas domésticas, incluyendo depósitos a la vista, depósitos de mercado monetario, demás depósitos de ahorro y depósitos a plazo",
    ),
    "The number of offices operated by an FDIC-insured institution in all commonwealths and terrirtories of the US, along with those in freely associated states under the Compact of Free Association": (
        "Número de agências operadas por instituição segurada pelo FDIC nos territórios e commonwealths dos Estados Unidos, incluindo as dos estados livremente associados sob o Compact of Free Association",
        "Número de oficinas operadas por una institución asegurada por la FDIC en los territorios y commonwealths de los Estados Unidos, incluyendo las de los estados libremente asociados bajo el Compact of Free Association",
    ),
    "The number of foreign offices (outside the U.S.) operated by the institution.": (
        "Número de agências no exterior (fora dos Estados Unidos) operadas pela instituição",
        "Número de oficinas en el exterior (fuera de los Estados Unidos) operadas por la institución",
    ),
    "The number of domestic offices (including headquarters) operated by active institutions in the 50 states of the U.S.A.": (
        "Número de agências domésticas (incluindo a sede) operadas por instituições ativas nos 50 estados dos Estados Unidos",
        "Número de oficinas domésticas (incluyendo la sede) operadas por instituciones activas en los 50 estados de los Estados Unidos",
    ),
    "The number of multiple service domestic offices operated by an institution.": (
        "Número de agências domésticas de serviço múltiplo operadas pela instituição",
        "Número de oficinas domésticas de servicio múltiple operadas por la institución",
    ),
    "The number of nondomestic offices operated by an institution.": (
        "Número de agências fora do território nacional operadas pela instituição",
        "Número de oficinas fuera del territorio nacional operadas por la institución",
    ),
    "The number of domestic non-multiple service offices operated by an institution.": (
        "Número de agências domésticas que não são de serviço múltiplo operadas pela instituição",
        "Número de oficinas domésticas que no son de servicio múltiple operadas por la institución",
    ),
    "The number of offices operated by an institution based on the summary of deposits definition of offices.": (
        "Número de agências operadas pela instituição segundo a definição de agência do Summary of Deposits",
        "Número de oficinas operadas por la institución según la definición de oficina del Summary of Deposits",
    ),
    "The total number of offices operated by an institution.": (
        "Número total de agências operadas pela instituição",
        "Número total de oficinas operadas por la institución",
    ),
    "The number of domestic and U.S terrirtories offices operated by an institution.": (
        "Número de agências domésticas e em territórios dos Estados Unidos operadas pela instituição",
        "Número de oficinas domésticas y en territorios de los Estados Unidos operadas por la institución",
    ),
    "The number of states with offices (including its main office).": (
        "Número de estados em que a instituição possui agências (incluindo a sede)",
        "Número de estados en que la institución posee oficinas (incluyendo la sede)",
    ),
    "Net income after taxes and extraordinary items (annualized) as a percent of average total assets.": (
        "Lucro líquido após impostos e itens extraordinários (anualizado) como percentual dos ativos totais médios",
        "Utilidad neta después de impuestos y partidas extraordinarias (anualizada) como porcentaje de los activos totales promedio",
    ),
    "Annualized net income as a percent of average equity on a consolidated basis.     Note: If retained earnings are  negative, the ratio is shown as NA.": (
        "Lucro líquido anualizado como percentual do patrimônio líquido médio em base consolidada. Nota: se os lucros acumulados forem negativos, o índice é apresentado como NA",
        "Utilidad neta anualizada como porcentaje del patrimonio neto promedio en base consolidada. Nota: si las utilidades acumuladas son negativas, el índice se presenta como NA",
    ),
    "Annualized pre-tax net income as a percent of average assets. Note: Includes extraordinary items and other adjustments, net of taxes.": (
        "Lucro líquido antes de impostos anualizado como percentual dos ativos médios. Nota: inclui itens extraordinários e outros ajustes, líquidos de impostos",
        "Utilidad neta antes de impuestos anualizada como porcentaje de los activos promedio. Nota: incluye partidas extraordinarias y otros ajustes, netos de impuestos",
    ),
    "Quarterly net income after taxes and extraordinary items as a percent of average total assets.": (
        "Lucro líquido do trimestre após impostos e itens extraordinários como percentual dos ativos totais médios",
        "Utilidad neta del trimestre después de impuestos y partidas extraordinarias como porcentaje de los activos totales promedio",
    ),
    "Quarterly net income (including gains or losses on securities and extraordinary items) as a percentage of average total equity capital.": (
        "Lucro líquido do trimestre (incluindo ganhos ou perdas com títulos e itens extraordinários) como percentual do patrimônio líquido total médio",
        "Utilidad neta del trimestre (incluyendo ganancias o pérdidas en valores y partidas extraordinarias) como porcentaje del patrimonio neto total promedio",
    ),
    "LOANS TO NONDEPOSITORY FINANCIAL INSTITUTIONS ON A CONSOLIDATED BANK BASIS. NOTE: 1) REPORTED IN THE CATEGORY OF LOANS TO NONDEPOSITORY FINANCIAL INSTITUTIONS AND ALL OTHER LOANS. 2) THIS ITEM IS NOT REPORTED BY FORM 041 OR 051 FILERS.": (
        "Empréstimos a instituições financeiras não depositárias em base bancária consolidada. Nota: 1) reportado na categoria de empréstimos a instituições financeiras não depositárias e demais empréstimos; 2) não é reportado por quem entrega os formulários 041 ou 051",
        "Préstamos a instituciones financieras no depositarias en base bancaria consolidada. Nota: 1) se reporta en la categoría de préstamos a instituciones financieras no depositarias y demás préstamos; 2) no lo reportan quienes presentan los formularios 041 o 051",
    ),
    "LOANS TO NONDEPOSITORY FINANCIAL INSTITUTIONS HELD IN DOMESTIC OFFICES.": (
        "Empréstimos a instituições financeiras não depositárias mantidos em agências domésticas",
        "Préstamos a instituciones financieras no depositarias mantenidos en oficinas domésticas",
    ),
    "TBD": ("A definir pelo FDIC", "Por definir por la FDIC"),
    # --- balance sheet: assets ---
    "Securities": ("Títulos e valores mobiliários", "Valores"),
    "Securities-Af": (
        "Títulos disponíveis para venda",
        "Valores disponibles para la venta",
    ),
    "Securities-Ha": (
        "Títulos mantidos até o vencimento",
        "Valores mantenidos hasta el vencimiento",
    ),
    "Securities-Mv": (
        "Títulos a valor de mercado",
        "Valores a valor de mercado",
    ),
    "Pledged Securities": (
        "Títulos dados em garantia",
        "Valores otorgados en garantía",
    ),
    "U.S. Treasury Securities": (
        "Títulos do Tesouro dos Estados Unidos",
        "Valores del Tesoro de los Estados Unidos",
    ),
    "U.S. Treasury & Agency": (
        "Títulos do Tesouro e de agências dos Estados Unidos",
        "Valores del Tesoro y de agencias de los Estados Unidos",
    ),
    "Municipal Securities": ("Títulos municipais", "Valores municipales"),
    "Priv Issued Res Mortgage-Backed Securities": (
        "Títulos privados lastreados em hipotecas residenciais",
        "Valores privados respaldados por hipotecas residenciales",
    ),
    "Equity Securities Not Held for Trading": (
        "Títulos patrimoniais não mantidos para negociação",
        "Valores patrimoniales no mantenidos para negociación",
    ),
    "Total Available-for-Sale At Amortized Cost Securities On A Consolidated Basis": (
        "Total de títulos disponíveis para venda ao custo amortizado, em base consolidada",
        "Total de valores disponibles para la venta a costo amortizado, en base consolidada",
    ),
    "Total Held-to-Maturity At Fair Value Securities On A Consolidated Basis": (
        "Total de títulos mantidos até o vencimento a valor justo, em base consolidada",
        "Total de valores mantenidos hasta el vencimiento a valor razonable, en base consolidada",
    ),
    "Trading Accounts": ("Contas de negociação", "Cuentas de negociación"),
    "Premises and Fixed Assets": (
        "Imóveis e ativos fixos",
        "Inmuebles y activos fijos",
    ),
    "Intangible Assets": ("Ativos intangíveis", "Activos intangibles"),
    "Other Assets": ("Demais ativos", "Demás activos"),
    "All Oth Assets": ("Demais ativos", "Demás activos"),
    "All Oth Assets Ratio": (
        "Demais ativos como proporção dos ativos totais",
        "Demás activos como proporción de los activos totales",
    ),
    "Other Real Estate Owned": (
        "Imóveis retomados e demais bens imóveis próprios",
        "Inmuebles adjudicados y demás bienes inmuebles propios",
    ),
    "Total Fiduciary and Related Assets": (
        "Total de ativos fiduciários e relacionados",
        "Total de activos fiduciarios y relacionados",
    ),
    "Number of Fiduciary Accounts and Related Asset Accounts": (
        "Número de contas fiduciárias e de contas de ativos relacionadas",
        "Número de cuentas fiduciarias y de cuentas de activos relacionadas",
    ),
    "Cash & Due From Depository Inst": (
        "Caixa e disponibilidades em instituições depositárias",
        "Efectivo y disponibilidades en instituciones depositarias",
    ),
    "Interest-Bearing Cash & Due": (
        "Disponibilidades remuneradas em instituições depositárias",
        "Disponibilidades remuneradas en instituciones depositarias",
    ),
    "Noninterest-Bearing Cash & Due": (
        "Disponibilidades não remuneradas em instituições depositárias",
        "Disponibilidades no remuneradas en instituciones depositarias",
    ),
    "Fed Funds & Repos Sold": (
        "Fed funds vendidos e operações compromissadas",
        "Fed funds vendidos y operaciones de reporto",
    ),
    "Avg Total Assets": ("Ativos totais médios", "Activos totales promedio"),
    "Average Assets-Adjusted-PCA": (
        "Ativos médios ajustados para fins do PCA",
        "Activos promedio ajustados para fines del PCA",
    ),
    "Long-Term Assets (5+ Years)-QBP": (
        "Ativos de longo prazo (5 anos ou mais), definição QBP",
        "Activos de largo plazo (5 años o más), definición QBP",
    ),
    "Earning Assets / Total Assets": (
        "Ativos rentáveis sobre ativos totais",
        "Activos rentables sobre activos totales",
    ),
    "Assets Per Employee in Million": (
        "Ativos por funcionário, em milhões de dólares",
        "Activos por empleado, en millones de dólares",
    ),
    # --- balance sheet: loans and leases ---
    "Real Estate Loans": (
        "Empréstimos imobiliários",
        "Préstamos inmobiliarios",
    ),
    "Real Estate Loans-Dom": (
        "Empréstimos imobiliários em agências domésticas",
        "Préstamos inmobiliarios en oficinas domésticas",
    ),
    "Real Estate Loans Adjusted": (
        "Empréstimos imobiliários ajustados",
        "Préstamos inmobiliarios ajustados",
    ),
    "Commercial and Industrial Loans": (
        "Empréstimos comerciais e industriais",
        "Préstamos comerciales e industriales",
    ),
    "Commercial & Industrial Loans": (
        "Empréstimos comerciais e industriais",
        "Préstamos comerciales e industriales",
    ),
    "Consumer Loans-Other": (
        "Demais empréstimos ao consumidor",
        "Demás préstamos al consumidor",
    ),
    "Agricultural Loans": ("Empréstimos agrícolas", "Préstamos agrícolas"),
    "Real Estate Construction & Land Develop": (
        "Empréstimos imobiliários para construção e loteamento",
        "Préstamos inmobiliarios para construcción y urbanización",
    ),
    "Real Estate Nonfarm Nonresidential Prop": (
        "Empréstimos imobiliários para imóveis não rurais e não residenciais",
        "Préstamos inmobiliarios para inmuebles no rurales y no residenciales",
    ),
    "Real Estate 1-4 Family": (
        "Empréstimos imobiliários residenciais de 1 a 4 unidades",
        "Préstamos inmobiliarios residenciales de 1 a 4 unidades",
    ),
    "Real Estate Multifamily": (
        "Empréstimos imobiliários multifamiliares",
        "Préstamos inmobiliarios multifamiliares",
    ),
    "Loans to Individuals": (
        "Empréstimos a pessoas físicas",
        "Préstamos a personas físicas",
    ),
    "Loans to Individuals Ratio": (
        "Empréstimos a pessoas físicas como proporção da carteira",
        "Préstamos a personas físicas como proporción de la cartera",
    ),
    "Loans to Depository Institutions and Acceptance of Other Banks": (
        "Empréstimos a instituições depositárias e aceites de outros bancos",
        "Préstamos a instituciones depositarias y aceptaciones de otros bancos",
    ),
    "Deposits Institution Loans": (
        "Empréstimos a instituições depositárias",
        "Préstamos a instituciones depositarias",
    ),
    "Executive Officer Loans-Amount": (
        "Empréstimos a diretores e administradores, valor",
        "Préstamos a directivos y administradores, monto",
    ),
    "All Other Loans & Leases (Including Farm)": (
        "Demais empréstimos e arrendamentos (incluindo rurais)",
        "Demás préstamos y arrendamientos (incluyendo rurales)",
    ),
    "All other loans & leases (including farm )": (
        "Demais empréstimos e arrendamentos (incluindo rurais)",
        "Demás préstamos y arrendamientos (incluyendo rurales)",
    ),
    "Loans and Leases-Total": (
        "Total de empréstimos e arrendamentos",
        "Total de préstamos y arrendamientos",
    ),
    "Loans and Leases-Total Adjusted": (
        "Total de empréstimos e arrendamentos, ajustado",
        "Total de préstamos y arrendamientos, ajustado",
    ),
    "Loans and Leases, Gross": (
        "Empréstimos e arrendamentos brutos",
        "Préstamos y arrendamientos brutos",
    ),
    "Loans and Leases-Net": (
        "Empréstimos e arrendamentos líquidos",
        "Préstamos y arrendamientos netos",
    ),
    "Loans & Leases + Unearned Inc": (
        "Empréstimos e arrendamentos mais receitas não realizadas",
        "Préstamos y arrendamientos más ingresos no devengados",
    ),
    "Leases": ("Arrendamentos mercantis", "Arrendamientos financieros"),
    "Unearned Income": ("Receitas não realizadas", "Ingresos no devengados"),
    "Allowance for Loan and Leases": (
        "Provisão para perdas com empréstimos e arrendamentos",
        "Provisión para pérdidas por préstamos y arrendamientos",
    ),
    "Allow for Loans Loss Adjusted": (
        "Provisão para perdas com empréstimos, ajustada",
        "Provisión para pérdidas por préstamos, ajustada",
    ),
    "Unused Commit-Total": (
        "Total de compromissos de crédito não utilizados",
        "Total de compromisos de crédito no utilizados",
    ),
    "Off-Balance Sheet Derivatives": (
        "Derivativos fora do balanço",
        "Derivados fuera de balance",
    ),
    "Interest Rate-Total Contracts": (
        "Total de contratos de taxa de juros",
        "Total de contratos de tasa de interés",
    ),
    # --- balance sheet: liabilities and capital ---
    "Total Liabilities": ("Passivos totais", "Pasivos totales"),
    "Total Liabilities & Capital": (
        "Passivos totais e patrimônio líquido",
        "Pasivos totales y patrimonio neto",
    ),
    "Trading Liabilities": (
        "Passivos de negociação",
        "Pasivos de negociación",
    ),
    "Oth Borrowed Funds": (
        "Demais recursos captados",
        "Demás recursos captados",
    ),
    "Total Equity Capital": (
        "Patrimônio líquido total",
        "Patrimonio neto total",
    ),
    "Bank Equity Capital/Assets": (
        "Patrimônio líquido sobre ativos totais",
        "Patrimonio neto sobre activos totales",
    ),
    "Sale of Capital Stock": ("Emissão de ações", "Emisión de acciones"),
    "Up-Net & Other Capital Comp": (
        "Demais componentes do patrimônio líquido",
        "Demás componentes del patrimonio neto",
    ),
    "Tier 1 Risk-Based Capital Ratio": (
        "Índice de capital de nível 1 ponderado pelo risco",
        "Índice de capital de nivel 1 ponderado por riesgo",
    ),
    "Common Equity Tier 1 Capital Ratio": (
        "Índice de capital principal de nível 1",
        "Índice de capital principal de nivel 1",
    ),
    "Total RBC Ratio-PCA": (
        "Índice de capital total ponderado pelo risco, definição PCA",
        "Índice de capital total ponderado por riesgo, definición PCA",
    ),
    "Tier 1 RBC Adjusted Llr - PCA": (
        "Capital de nível 1 ajustado pela provisão para perdas, definição PCA",
        "Capital de nivel 1 ajustado por la provisión para pérdidas, definición PCA",
    ),
    "Leverage Ratio-PCA": (
        "Índice de alavancagem, definição PCA",
        "Índice de apalancamiento, definición PCA",
    ),
    "Community Bank Ratio": (
        "Índice de alavancagem simplificado para bancos comunitários",
        "Índice de apalancamiento simplificado para bancos comunitarios",
    ),
    # --- deposits ---
    "Noninterest-Bearing Deposits": (
        "Depósitos não remunerados",
        "Depósitos no remunerados",
    ),
    "Noninterest-Bearing Deposits-Dom": (
        "Depósitos não remunerados em agências domésticas",
        "Depósitos no remunerados en oficinas domésticas",
    ),
    "Interest-Bearing Deposits": (
        "Depósitos remunerados",
        "Depósitos remunerados",
    ),
    "Interest-Bearing Deposits-Dom": (
        "Depósitos remunerados em agências domésticas",
        "Depósitos remunerados en oficinas domésticas",
    ),
    "Total Deposits-for": (
        "Depósitos em agências no exterior",
        "Depósitos en oficinas del exterior",
    ),
    "Core Deposits": ("Depósitos estáveis", "Depósitos estables"),
    "Estimated Insured Deposits": (
        "Depósitos segurados estimados",
        "Depósitos asegurados estimados",
    ),
    "Estimated Uninsured Deposits in Domestic Offices and in Insured Branches in US Territories and Possessions": (
        "Depósitos não segurados estimados em agências domésticas e em agências seguradas nos territórios e possessões dos Estados Unidos",
        "Depósitos no asegurados estimados en oficinas domésticas y en sucursales aseguradas en los territorios y posesiones de los Estados Unidos",
    ),
    "Est Uninsured Deposits in Dom-Off in Insured Branches in US Terr and Possessions": (
        "Depósitos não segurados estimados em agências domésticas e em agências seguradas nos territórios e possessões dos Estados Unidos",
        "Depósitos no asegurados estimados en oficinas domésticas y en sucursales aseguradas en los territorios y posesiones de los Estados Unidos",
    ),
    "Time Deposits Over $100M": (
        "Depósitos a prazo acima de 100 mil dólares",
        "Depósitos a plazo superiores a 100 mil dólares",
    ),
    "Num Deposits Acc Equal or Less Than Equal to $250,000": (
        "Número de contas de depósito de até 250 mil dólares",
        "Número de cuentas de depósito de hasta 250 mil dólares",
    ),
    "Num Deposits Acc Greater Than $250,000": (
        "Número de contas de depósito acima de 250 mil dólares",
        "Número de cuentas de depósito superiores a 250 mil dólares",
    ),
    "Iras and Keogh Plans-Deposits": (
        "Depósitos em planos IRA e Keogh",
        "Depósitos en planes IRA y Keogh",
    ),
    "Deposit Liabilities After Exclusions": (
        "Passivos de depósito após exclusões",
        "Pasivos por depósitos después de exclusiones",
    ),
    "Total Deposit Liab Bef Exclusion": (
        "Passivos de depósito antes das exclusões",
        "Pasivos por depósitos antes de las exclusiones",
    ),
    "Total Allowable Exclusions (Including Foreign Deposits)": (
        "Total de exclusões admissíveis (incluindo depósitos no exterior)",
        "Total de exclusiones admisibles (incluyendo depósitos en el exterior)",
    ),
    "Tot Domestic Deposit / Asset": (
        "Depósitos domésticos sobre ativos totais",
        "Depósitos domésticos sobre activos totales",
    ),
    # --- income statement ---
    "Total Interest Income": (
        "Receita total de juros",
        "Ingreso total por intereses",
    ),
    "Total Interest Income Quarterly": (
        "Receita total de juros no trimestre",
        "Ingreso total por intereses del trimestre",
    ),
    "Total Interest Expense": (
        "Despesa total de juros",
        "Gasto total por intereses",
    ),
    "Total Interest Expense Annually": (
        "Despesa total de juros no ano",
        "Gasto total por intereses del año",
    ),
    "Total Interest Expense Quarterly": (
        "Despesa total de juros no trimestre",
        "Gasto total por intereses del trimestre",
    ),
    "Net Interest Income": (
        "Receita líquida de juros",
        "Ingreso neto por intereses",
    ),
    "Net Interest Margin": (
        "Margem financeira líquida",
        "Margen financiero neto",
    ),
    "Deposit Interest Expense": (
        "Despesa de juros sobre depósitos",
        "Gasto por intereses sobre depósitos",
    ),
    "Deposit Interest Expense-Dom": (
        "Despesa de juros sobre depósitos domésticos",
        "Gasto por intereses sobre depósitos domésticos",
    ),
    "Deposit Interest Expense-Dom Quarterly": (
        "Despesa de juros sobre depósitos domésticos no trimestre",
        "Gasto por intereses sobre depósitos domésticos del trimestre",
    ),
    "Total Noninterest Income": (
        "Receita total não decorrente de juros",
        "Ingreso total no derivado de intereses",
    ),
    "Total Noninterest Expense": (
        "Despesa total não decorrente de juros",
        "Gasto total no derivado de intereses",
    ),
    "Additional Noninterest Income": (
        "Demais receitas não decorrentes de juros",
        "Demás ingresos no derivados de intereses",
    ),
    "Additional Noninterest Income Quarterly": (
        "Demais receitas não decorrentes de juros no trimestre",
        "Demás ingresos no derivados de intereses del trimestre",
    ),
    "Additional Noninterest Expense": (
        "Demais despesas não decorrentes de juros",
        "Demás gastos no derivados de intereses",
    ),
    "Additional Noninterest Expense Quarterly": (
        "Demais despesas não decorrentes de juros no trimestre",
        "Demás gastos no derivados de intereses del trimestre",
    ),
    "All Other Noninterest Expense": (
        "Demais despesas não decorrentes de juros",
        "Demás gastos no derivados de intereses",
    ),
    "All Other Noninterest Expense Quarterly": (
        "Demais despesas não decorrentes de juros no trimestre",
        "Demás gastos no derivados de intereses del trimestre",
    ),
    "Salaries and Employee Benefits": (
        "Salários e benefícios a funcionários",
        "Salarios y beneficios a empleados",
    ),
    "Salaries and Employee Benefits Quarterly": (
        "Salários e benefícios a funcionários no trimestre",
        "Salarios y beneficios a empleados del trimestre",
    ),
    "Premises & Fixed Assets Expense": (
        "Despesa com imóveis e ativos fixos",
        "Gasto por inmuebles y activos fijos",
    ),
    "Premises & Fixed Assets Expense Quarterly": (
        "Despesa com imóveis e ativos fixos no trimestre",
        "Gasto por inmuebles y activos fijos del trimestre",
    ),
    "Securities Gains and Losses": (
        "Ganhos e perdas com títulos",
        "Ganancias y pérdidas en valores",
    ),
    "Total Security Income": (
        "Receita total com títulos",
        "Ingreso total por valores",
    ),
    "Total Security Income-Ann": (
        "Receita total com títulos no ano",
        "Ingreso total por valores del año",
    ),
    "Total Security Income Quarterly": (
        "Receita total com títulos no trimestre",
        "Ingreso total por valores del trimestre",
    ),
    "Loan Income-Ann": (
        "Receita com empréstimos no ano",
        "Ingreso por préstamos del año",
    ),
    "Loan Income-Dom": (
        "Receita com empréstimos em agências domésticas",
        "Ingreso por préstamos en oficinas domésticas",
    ),
    "Loan Income-Dom Quarterly": (
        "Receita com empréstimos em agências domésticas no trimestre",
        "Ingreso por préstamos en oficinas domésticas del trimestre",
    ),
    "Loan Income-Qtr": (
        "Receita com empréstimos no trimestre",
        "Ingreso por préstamos del trimestre",
    ),
    "Loan & Lease Income-Ann": (
        "Receita com empréstimos e arrendamentos no ano",
        "Ingreso por préstamos y arrendamientos del año",
    ),
    "Loan & Lease Income-Qtr": (
        "Receita com empréstimos e arrendamentos no trimestre",
        "Ingreso por préstamos y arrendamientos del trimestre",
    ),
    "Provisions for Credit Losses": (
        "Provisões para perdas de crédito",
        "Provisiones para pérdidas crediticias",
    ),
    "Applicable Income Taxes": (
        "Impostos sobre a renda aplicáveis",
        "Impuestos sobre la renta aplicables",
    ),
    "Applicable Income Taxes-Ann": (
        "Impostos sobre a renda aplicáveis no ano",
        "Impuestos sobre la renta aplicables del año",
    ),
    "Applicable Income Taxes Quarterly": (
        "Impostos sobre a renda aplicáveis no trimestre",
        "Impuestos sobre la renta aplicables del trimestre",
    ),
    "Applicable Income Taxes-Qtr-Ann": (
        "Impostos sobre a renda aplicáveis no trimestre, anualizados",
        "Impuestos sobre la renta aplicables del trimestre, anualizados",
    ),
    "Income Before Inc Taxes & Disc": (
        "Resultado antes de impostos e operações descontinuadas",
        "Resultado antes de impuestos y operaciones descontinuadas",
    ),
    "Income Before Disc Opr": (
        "Resultado antes de operações descontinuadas",
        "Resultado antes de operaciones descontinuadas",
    ),
    "Net Discontinued Operations": (
        "Resultado líquido de operações descontinuadas",
        "Resultado neto de operaciones descontinuadas",
    ),
    "Net income - quarterly": (
        "Lucro líquido do trimestre",
        "Utilidad neta del trimestre",
    ),
    "Net Inc - Bank & Minority Int": (
        "Lucro líquido do banco e de participações minoritárias",
        "Utilidad neta del banco y de participaciones minoritarias",
    ),
    "Net Operating Income-Adj": (
        "Resultado operacional líquido ajustado",
        "Resultado operativo neto ajustado",
    ),
    "Net Operating Income-Adj/Assets": (
        "Resultado operacional líquido ajustado sobre ativos",
        "Resultado operativo neto ajustado sobre activos",
    ),
    "Net Operating Income-Qtr": (
        "Resultado operacional líquido do trimestre",
        "Resultado operativo neto del trimestre",
    ),
    "Pre-Tax Net Income Operating Income": (
        "Resultado operacional antes de impostos",
        "Resultado operativo antes de impuestos",
    ),
    "Pre-Tax Net Income Operating Income Quarterly": (
        "Resultado operacional antes de impostos no trimestre",
        "Resultado operativo antes de impuestos del trimestre",
    ),
    "Net Operating Cash Flow-Ann": (
        "Fluxo de caixa operacional líquido no ano",
        "Flujo de caja operativo neto del año",
    ),
    "NET OPERATING CASH FLOW-ANN Quarterly": (
        "Fluxo de caixa operacional líquido no trimestre",
        "Flujo de caja operativo neto del trimestre",
    ),
    "Cash Dividends On Comm & Pref": (
        "Dividendos pagos sobre ações ordinárias e preferenciais",
        "Dividendos pagados sobre acciones ordinarias y preferentes",
    ),
    "Cash Dividends On Comm & Pref Quarterly": (
        "Dividendos pagos sobre ações ordinárias e preferenciais no trimestre",
        "Dividendos pagados sobre acciones ordinarias y preferentes del trimestre",
    ),
    "Cash Dividends to Net Income (YTD Only)": (
        "Dividendos pagos sobre o lucro líquido (acumulado no ano)",
        "Dividendos pagados sobre la utilidad neta (acumulado del año)",
    ),
    "Retained Earnings/Avg Bk Equity": (
        "Lucros retidos sobre o patrimônio líquido médio",
        "Utilidades retenidas sobre el patrimonio neto promedio",
    ),
    # --- performance ratios ---
    "Efficiency Ratio": ("Índice de eficiência", "Índice de eficiencia"),
    "Efficiency Ratio Expense": (
        "Despesas consideradas no índice de eficiência",
        "Gastos considerados en el índice de eficiencia",
    ),
    "Interest Income to Earning Assets Ratio": (
        "Receita de juros sobre ativos rentáveis",
        "Ingreso por intereses sobre activos rentables",
    ),
    "Interest Expense to Earning Assets Ratio": (
        "Despesa de juros sobre ativos rentáveis",
        "Gasto por intereses sobre activos rentables",
    ),
    "Cost of Funding Earning Assets Quarterly": (
        "Custo de captação dos ativos rentáveis no trimestre",
        "Costo de captación de los activos rentables del trimestre",
    ),
    "Noninterest Exp/Average Assets": (
        "Despesa não decorrente de juros sobre ativos médios",
        "Gasto no derivado de intereses sobre activos promedio",
    ),
    "Noninterest Inc/Average Assets": (
        "Receita não decorrente de juros sobre ativos médios",
        "Ingreso no derivado de intereses sobre activos promedio",
    ),
    "Credit Loss Prov/Ave Assets": (
        "Provisão para perdas de crédito sobre ativos médios",
        "Provisión para pérdidas crediticias sobre activos promedio",
    ),
    "Net Loans & Leases/Deposits": (
        "Empréstimos e arrendamentos líquidos sobre depósitos",
        "Préstamos y arrendamientos netos sobre depósitos",
    ),
    "Net Loans & Leases/Assets": (
        "Empréstimos e arrendamentos líquidos sobre ativos",
        "Préstamos y arrendamientos netos sobre activos",
    ),
    "Net Loans and Leases to Core Deposits Ratio": (
        "Empréstimos e arrendamentos líquidos sobre depósitos estáveis",
        "Préstamos y arrendamientos netos sobre depósitos estables",
    ),
    "Commercial & Industrial Loans Ratio": (
        "Empréstimos comerciais e industriais como proporção da carteira",
        "Préstamos comerciales e industriales como proporción de la cartera",
    ),
    "Earnings Coverage of Net Loan Charge-Offs (X)": (
        "Cobertura das baixas líquidas por perda pelos resultados, em vezes",
        "Cobertura de los castigos netos por pérdida con los resultados, en veces",
    ),
    # --- asset quality ---
    "Total Loans & Leases Charge-Offs": (
        "Baixas por perda de empréstimos e arrendamentos",
        "Castigos por pérdida de préstamos y arrendamientos",
    ),
    "Total Loans & Leases Charge-Offs Quarterly": (
        "Baixas por perda de empréstimos e arrendamentos no trimestre",
        "Castigos por pérdida de préstamos y arrendamientos del trimestre",
    ),
    "Total Loans & Leases Recoveries": (
        "Recuperações de empréstimos e arrendamentos",
        "Recuperaciones de préstamos y arrendamientos",
    ),
    "Total Loans & Leases Recoveries Quarterly": (
        "Recuperações de empréstimos e arrendamentos no trimestre",
        "Recuperaciones de préstamos y arrendamientos del trimestre",
    ),
    "Total Loans & Leases Net Charge-Offs": (
        "Baixas líquidas por perda de empréstimos e arrendamentos",
        "Castigos netos por pérdida de préstamos y arrendamientos",
    ),
    "Net Charge-Offs/Loans & Leases": (
        "Baixas líquidas por perda sobre empréstimos e arrendamentos",
        "Castigos netos por pérdida sobre préstamos y arrendamientos",
    ),
    "Net Charge-offs All other loans & leases (including farm) Numerator": (
        "Baixas líquidas por perda dos demais empréstimos e arrendamentos (incluindo rurais), numerador",
        "Castigos netos por pérdida de los demás préstamos y arrendamientos (incluyendo rurales), numerador",
    ),
    "Loan Loss Reserve/Gross Loans & Leases": (
        "Provisão para perdas sobre empréstimos e arrendamentos brutos",
        "Provisión para pérdidas sobre préstamos y arrendamientos brutos",
    ),
    "Loan Loss Reserve/N/C Loans": (
        "Provisão para perdas sobre empréstimos inadimplentes",
        "Provisión para pérdidas sobre préstamos en incumplimiento",
    ),
    "Loan Loss Prov/Nt Chg-Offs": (
        "Provisão para perdas sobre baixas líquidas por perda",
        "Provisión para pérdidas sobre castigos netos por pérdida",
    ),
    "Nonperf Assets/Total Assets": (
        "Ativos inadimplentes sobre ativos totais",
        "Activos en incumplimiento sobre activos totales",
    ),
    "Nonaccrual-Total Assets": (
        "Ativos em não acumulação de juros sobre ativos totais",
        "Activos en no acumulación de intereses sobre activos totales",
    ),
    "Nonaccrual-Loans & Leases": (
        "Empréstimos e arrendamentos em não acumulação de juros",
        "Préstamos y arrendamientos en no acumulación de intereses",
    ),
    "Nonaccrual-Commercial and Industrial Loans": (
        "Empréstimos comerciais e industriais em não acumulação de juros",
        "Préstamos comerciales e industriales en no acumulación de intereses",
    ),
    "Nonaccrual Total Loans - Loss Sh": (
        "Empréstimos em não acumulação de juros sob acordo de perda compartilhada",
        "Préstamos en no acumulación de intereses bajo acuerdo de pérdida compartida",
    ),
    "Total N/C-Loans & Leases": (
        "Total de empréstimos e arrendamentos inadimplentes",
        "Total de préstamos y arrendamientos en incumplimiento",
    ),
    "N/C Lns & Ls/Gross Lns & Ls": (
        "Empréstimos e arrendamentos inadimplentes sobre a carteira bruta",
        "Préstamos y arrendamientos en incumplimiento sobre la cartera bruta",
    ),
    "90+ Days P/D-Total Assets": (
        "Ativos com atraso de 90 dias ou mais sobre ativos totais",
        "Activos con mora de 90 días o más sobre activos totales",
    ),
    "30-89 Days P/D-Total Assets": (
        "Ativos com atraso de 30 a 89 dias sobre ativos totais",
        "Activos con mora de 30 a 89 días sobre activos totales",
    ),
    "90+ Days P/D-Loans & Leases": (
        "Empréstimos e arrendamentos com atraso de 90 dias ou mais",
        "Préstamos y arrendamientos con mora de 90 días o más",
    ),
    "90+ D P/D Total Loans - Loss Sh": (
        "Empréstimos com atraso de 90 dias ou mais sob acordo de perda compartilhada",
        "Préstamos con mora de 90 días o más bajo acuerdo de pérdida compartida",
    ),
    "30-89 D P/D Total Loans-Loss Sh": (
        "Empréstimos com atraso de 30 a 89 dias sob acordo de perda compartilhada",
        "Préstamos con mora de 30 a 89 días bajo acuerdo de pérdida compartida",
    ),
    "90+ Real Estate Loans in Domestic Offices": (
        "Empréstimos imobiliários em agências domésticas com atraso de 90 dias ou mais",
        "Préstamos inmobiliarios en oficinas domésticas con mora de 90 días o más",
    ),
    "P/D 30-89 Real Estate Loans in Domestic Offices": (
        "Empréstimos imobiliários em agências domésticas com atraso de 30 a 89 dias",
        "Préstamos inmobiliarios en oficinas domésticas con mora de 30 a 89 días",
    ),
    "Noncurrent Loans Which Are Wholly or Partially Guaranteed By The U.S. Government Ratio": (
        "Empréstimos inadimplentes total ou parcialmente garantidos pelo governo dos Estados Unidos, como proporção da carteira",
        "Préstamos en incumplimiento total o parcialmente garantizados por el gobierno de los Estados Unidos, como proporción de la cartera",
    ),
    "Carry Amt Loss Share- Ore": (
        "Valor contábil de imóveis retomados sob acordo de perda compartilhada",
        "Valor contable de inmuebles adjudicados bajo acuerdo de pérdida compartida",
    ),
    "Carry Amt Loss Share-Lnls": (
        "Valor contábil de empréstimos e arrendamentos sob acordo de perda compartilhada",
        "Valor contable de préstamos y arrendamientos bajo acuerdo de pérdida compartida",
    ),
    "Carry Amt Loss Share -Oth Asset": (
        "Valor contábil dos demais ativos sob acordo de perda compartilhada",
        "Valor contable de los demás activos bajo acuerdo de pérdida compartida",
    ),
    "Carry Amt Loss Share -Debt Sec": (
        "Valor contábil de títulos de dívida sob acordo de perda compartilhada",
        "Valor contable de valores de deuda bajo acuerdo de pérdida compartida",
    ),
    "Sold W/Recourse N/Secur. - Oth": (
        "Demais ativos vendidos com coobrigação e não securitizados",
        "Demás activos vendidos con recurso y no securitizados",
    ),
    "Sold W/Recourse N/Secur.- Res": (
        "Ativos residenciais vendidos com coobrigação e não securitizados",
        "Activos residenciales vendidos con recurso y no securitizados",
    ),
    "Real Estate Principal Securitised Asset Sold - Cons": (
        "Principal de ativos de crédito ao consumidor securitizados e vendidos",
        "Principal de activos de crédito al consumidor securitizados y vendidos",
    ),
    "Real Estate Principal Securitised Asset Sold-Res": (
        "Principal de ativos imobiliários residenciais securitizados e vendidos",
        "Principal de activos inmobiliarios residenciales securitizados y vendidos",
    ),
    "Real Estate Principal Securitised Asset Sold - Auto": (
        "Principal de ativos de financiamento de veículos securitizados e vendidos",
        "Principal de activos de financiamiento de vehículos securitizados y vendidos",
    ),
    "Real Estate Principal Securitised Asset Sold - Hel": (
        "Principal de ativos de crédito com garantia imobiliária securitizados e vendidos",
        "Principal de activos de crédito con garantía inmobiliaria securitizados y vendidos",
    ),
    "Real Estate Principal Securitised Asset Sold - Crcd": (
        "Principal de ativos de cartão de crédito securitizados e vendidos",
        "Principal de activos de tarjeta de crédito securitizados y vendidos",
    ),
    "Real Estate Principal Securitised Asset Sold - Ci": (
        "Principal de ativos comerciais e industriais securitizados e vendidos",
        "Principal de activos comerciales e industriales securitizados y vendidos",
    ),
    "Real Estate Principal Securitised Asset Sold - Oth": (
        "Principal dos demais ativos securitizados e vendidos",
        "Principal de los demás activos securitizados y vendidos",
    ),
    "Number of Full Time Employees": (
        "Número de funcionários em tempo integral",
        "Número de empleados de tiempo completo",
    ),
    "Federal Reserve Id Number": (
        "Número de identificação no Federal Reserve",
        "Número de identificación en la Reserva Federal",
    ),
    # --- charge-off and non-current ratios by collateral type ---
    "Nonresidential Charge-Off/Nonresidential Loans": (
        "Baixas por perda sobre empréstimos imobiliários não residenciais",
        "Castigos por pérdida sobre préstamos inmobiliarios no residenciales",
    ),
    "Multifamily Real Estate Charge-Off/Multi Real Estate Ln": (
        "Baixas por perda sobre empréstimos imobiliários multifamiliares",
        "Castigos por pérdida sobre préstamos inmobiliarios multifamiliares",
    ),
    "Commercial Real Estate Charge-Off/Comm Real Estate Ln": (
        "Baixas por perda sobre empréstimos imobiliários comerciais",
        "Castigos por pérdida sobre préstamos inmobiliarios comerciales",
    ),
    "Real Estate Charge-Off/Real Estate Loans": (
        "Baixas por perda sobre empréstimos imobiliários",
        "Castigos por pérdida sobre préstamos inmobiliarios",
    ),
    "Const Real Estate Charge-Off/Const Real Estate Loans": (
        "Baixas por perda sobre empréstimos imobiliários para construção",
        "Castigos por pérdida sobre préstamos inmobiliarios para construcción",
    ),
    "1-4 Fam Real Estate Charge-Off/1-4 Fam Loans": (
        "Baixas por perda sobre empréstimos imobiliários residenciais de 1 a 4 unidades",
        "Castigos por pérdida sobre préstamos inmobiliarios residenciales de 1 a 4 unidades",
    ),
    "1-4 Fam Real Estate Charge-Off/1-4 Fam Loans Quarterly Ratio": (
        "Baixas por perda sobre empréstimos imobiliários residenciais de 1 a 4 unidades, no trimestre",
        "Castigos por pérdida sobre préstamos inmobiliarios residenciales de 1 a 4 unidades, del trimestre",
    ),
    "Real Estate Loan Net Charge-Offs Quarterly Ratio": (
        "Baixas líquidas por perda sobre empréstimos imobiliários, no trimestre",
        "Castigos netos por pérdida sobre préstamos inmobiliarios, del trimestre",
    ),
    "N/C Multifamly Real Estate/Multifamly Real Estate": (
        "Empréstimos imobiliários multifamiliares inadimplentes sobre o total dessa carteira",
        "Préstamos inmobiliarios multifamiliares en incumplimiento sobre el total de esa cartera",
    ),
    "N/C Const Real Estate/Const Real Estate": (
        "Empréstimos imobiliários para construção inadimplentes sobre o total dessa carteira",
        "Préstamos inmobiliarios para construcción en incumplimiento sobre el total de esa cartera",
    ),
    "N/C Nonfarm Nonresidential Real Estate/Nonresidential Real Estate": (
        "Empréstimos imobiliários não rurais e não residenciais inadimplentes sobre o total dessa carteira",
        "Préstamos inmobiliarios no rurales y no residenciales en incumplimiento sobre el total de esa cartera",
    ),
    "N/C Real Estate Lns/Real Estate": (
        "Empréstimos imobiliários inadimplentes sobre o total da carteira imobiliária",
        "Préstamos inmobiliarios en incumplimiento sobre el total de la cartera inmobiliaria",
    ),
    "N/C 1-4 Family Real Estate/1-4 Family Real Estate": (
        "Empréstimos imobiliários residenciais de 1 a 4 unidades inadimplentes sobre o total dessa carteira",
        "Préstamos inmobiliarios residenciales de 1 a 4 unidades en incumplimiento sobre el total de esa cartera",
    ),
    "Nc Commercial Real Estate/Commercial Real Estate": (
        "Empréstimos imobiliários comerciais inadimplentes sobre o total dessa carteira",
        "Préstamos inmobiliarios comerciales en incumplimiento sobre el total de esa cartera",
    ),
    # --- recoveries by loan category ---
    "Commercial Loan Recoveries": (
        "Recuperações de empréstimos comerciais",
        "Recuperaciones de préstamos comerciales",
    ),
    "Commercial Loan Recoveries Quarterly": (
        "Recuperações de empréstimos comerciais no trimestre",
        "Recuperaciones de préstamos comerciales del trimestre",
    ),
    "Other Consumer Loan Recoveries": (
        "Recuperações dos demais empréstimos ao consumidor",
        "Recuperaciones de los demás préstamos al consumidor",
    ),
    "Other Consumer Loan Recoveries Quarterly": (
        "Recuperações dos demais empréstimos ao consumidor no trimestre",
        "Recuperaciones de los demás préstamos al consumidor del trimestre",
    ),
    "Consumer Loan Recoveries Quarterly": (
        "Recuperações de empréstimos ao consumidor no trimestre",
        "Recuperaciones de préstamos al consumidor del trimestre",
    ),
    "Credit Card Loan Recoveries": (
        "Recuperações de empréstimos de cartão de crédito",
        "Recuperaciones de préstamos de tarjeta de crédito",
    ),
    "Credit Card Loan Recoveries Quarterly": (
        "Recuperações de empréstimos de cartão de crédito no trimestre",
        "Recuperaciones de préstamos de tarjeta de crédito del trimestre",
    ),
    "Lease Recoveries": (
        "Recuperações de arrendamentos",
        "Recuperaciones de arrendamientos",
    ),
    "Lease Recoveries Quarterly": (
        "Recuperações de arrendamentos no trimestre",
        "Recuperaciones de arrendamientos del trimestre",
    ),
    "Real Estate Loan Recoveries": (
        "Recuperações de empréstimos imobiliários",
        "Recuperaciones de préstamos inmobiliarios",
    ),
    "Real Estate Loan Recoveries Quarterly": (
        "Recuperações de empréstimos imobiliários no trimestre",
        "Recuperaciones de préstamos inmobiliarios del trimestre",
    ),
    "Real Estate Loan Recoveries Domestic Offices": (
        "Recuperações de empréstimos imobiliários em agências domésticas",
        "Recuperaciones de préstamos inmobiliarios en oficinas domésticas",
    ),
    "Real Estate Loan Recoveries Domestic Offices Quarterly": (
        "Recuperações de empréstimos imobiliários em agências domésticas no trimestre",
        "Recuperaciones de préstamos inmobiliarios en oficinas domésticas del trimestre",
    ),
    "Farmland Real Estate Ln Recoveries": (
        "Recuperações de empréstimos imobiliários rurais",
        "Recuperaciones de préstamos inmobiliarios rurales",
    ),
    "Farmland Real Estate Ln Recoveries-Qtr": (
        "Recuperações de empréstimos imobiliários rurais no trimestre",
        "Recuperaciones de préstamos inmobiliarios rurales del trimestre",
    ),
    "Construction Real Estate Ln Recoveries": (
        "Recuperações de empréstimos imobiliários para construção",
        "Recuperaciones de préstamos inmobiliarios para construcción",
    ),
    "Construction Real Estate Ln Recover-Qtr": (
        "Recuperações de empréstimos imobiliários para construção no trimestre",
        "Recuperaciones de préstamos inmobiliarios para construcción del trimestre",
    ),
    "Line of Credit Real Estate Ln Recoveries": (
        "Recuperações de linhas de crédito com garantia imobiliária",
        "Recuperaciones de líneas de crédito con garantía inmobiliaria",
    ),
    "Line of Credit Real Estate Ln Recoveries Quarterly": (
        "Recuperações de linhas de crédito com garantia imobiliária no trimestre",
        "Recuperaciones de líneas de crédito con garantía inmobiliaria del trimestre",
    ),
    "Multifamily Real Estate Ln Recoveries-Qtr": (
        "Recuperações de empréstimos imobiliários multifamiliares no trimestre",
        "Recuperaciones de préstamos inmobiliarios multifamiliares del trimestre",
    ),
    "Multifamily Res Real Estate Ln Recoveries": (
        "Recuperações de empréstimos imobiliários residenciais multifamiliares",
        "Recuperaciones de préstamos inmobiliarios residenciales multifamiliares",
    ),
    "Nonfarm Nonresidential Real Estate Ln Recoveries": (
        "Recuperações de empréstimos imobiliários não rurais e não residenciais",
        "Recuperaciones de préstamos inmobiliarios no rurales y no residenciales",
    ),
    "Nonfarm Nonresidential Real Estate Ln Recover-Qtr": (
        "Recuperações de empréstimos imobiliários não rurais e não residenciais no trimestre",
        "Recuperaciones de préstamos inmobiliarios no rurales y no residenciales del trimestre",
    ),
    "Real Estate Loans 1-4 Family Recoveries": (
        "Recuperações de empréstimos imobiliários residenciais de 1 a 4 unidades",
        "Recuperaciones de préstamos inmobiliarios residenciales de 1 a 4 unidades",
    ),
    "Real Estate Loans 1-4 Family Recover-Qtr": (
        "Recuperações de empréstimos imobiliários residenciais de 1 a 4 unidades no trimestre",
        "Recuperaciones de préstamos inmobiliarios residenciales de 1 a 4 unidades del trimestre",
    ),
    # --- charge-offs by loan category ---
    "Commercial Loan Charge-Offs": (
        "Baixas por perda de empréstimos comerciais",
        "Castigos por pérdida de préstamos comerciales",
    ),
    "Commercial Loan Charge-Offs Quarterly": (
        "Baixas por perda de empréstimos comerciais no trimestre",
        "Castigos por pérdida de préstamos comerciales del trimestre",
    ),
    "Other Consumer Loan Charge-Offs": (
        "Baixas por perda dos demais empréstimos ao consumidor",
        "Castigos por pérdida de los demás préstamos al consumidor",
    ),
    "Other Consumer Loan Charge-Offs Quarterly": (
        "Baixas por perda dos demais empréstimos ao consumidor no trimestre",
        "Castigos por pérdida de los demás préstamos al consumidor del trimestre",
    ),
    "Consumer Loan Charge-Offs Quarterly": (
        "Baixas por perda de empréstimos ao consumidor no trimestre",
        "Castigos por pérdida de préstamos al consumidor del trimestre",
    ),
    "Credit Card Loan Charge-Offs": (
        "Baixas por perda de empréstimos de cartão de crédito",
        "Castigos por pérdida de préstamos de tarjeta de crédito",
    ),
    "Credit Card Loan Charge-Offs Quarterly": (
        "Baixas por perda de empréstimos de cartão de crédito no trimestre",
        "Castigos por pérdida de préstamos de tarjeta de crédito del trimestre",
    ),
    "Lease Charge-Offs": (
        "Baixas por perda de arrendamentos",
        "Castigos por pérdida de arrendamientos",
    ),
    "Lease Charge-Offs Quarterly": (
        "Baixas por perda de arrendamentos no trimestre",
        "Castigos por pérdida de arrendamientos del trimestre",
    ),
    "Real Estate Loan Charge-Offs": (
        "Baixas por perda de empréstimos imobiliários",
        "Castigos por pérdida de préstamos inmobiliarios",
    ),
    "Real Estate Loan Charge-Offs Quarterly": (
        "Baixas por perda de empréstimos imobiliários no trimestre",
        "Castigos por pérdida de préstamos inmobiliarios del trimestre",
    ),
    "Real Estate Loan Charge-Offs Domestic Offices": (
        "Baixas por perda de empréstimos imobiliários em agências domésticas",
        "Castigos por pérdida de préstamos inmobiliarios en oficinas domésticas",
    ),
    "Real Estate Loan Charge-Offs Domestic Offices Quarterly": (
        "Baixas por perda de empréstimos imobiliários em agências domésticas no trimestre",
        "Castigos por pérdida de préstamos inmobiliarios en oficinas domésticas del trimestre",
    ),
    "Real Estate Loan Net Charge-Offs Domestic Offices": (
        "Baixas líquidas por perda de empréstimos imobiliários em agências domésticas",
        "Castigos netos por pérdida de préstamos inmobiliarios en oficinas domésticas",
    ),
    "Real Estate Loan Net Charge-Offs Domestic Offices Quarterly": (
        "Baixas líquidas por perda de empréstimos imobiliários em agências domésticas no trimestre",
        "Castigos netos por pérdida de préstamos inmobiliarios en oficinas domésticas del trimestre",
    ),
    "Farmland Real Estate Ln Charge-Offs": (
        "Baixas por perda de empréstimos imobiliários rurais",
        "Castigos por pérdida de préstamos inmobiliarios rurales",
    ),
    "Construction Real Estate Ln Charge-Offs": (
        "Baixas por perda de empréstimos imobiliários para construção",
        "Castigos por pérdida de préstamos inmobiliarios para construcción",
    ),
    "Line of Credit Real Estate Ln Charge-Offs": (
        "Baixas por perda de linhas de crédito com garantia imobiliária",
        "Castigos por pérdida de líneas de crédito con garantía inmobiliaria",
    ),
    "Line of Credit Real Estate Ln Charge-Offs Quarterly": (
        "Baixas por perda de linhas de crédito com garantia imobiliária no trimestre",
        "Castigos por pérdida de líneas de crédito con garantía inmobiliaria del trimestre",
    ),
    "Multifamily Res Real Estate Ln Charge-Off": (
        "Baixas por perda de empréstimos imobiliários residenciais multifamiliares",
        "Castigos por pérdida de préstamos inmobiliarios residenciales multifamiliares",
    ),
    "Nonfarm Nonresidential Real Estate Ln Charge-Offs": (
        "Baixas por perda de empréstimos imobiliários não rurais e não residenciais",
        "Castigos por pérdida de préstamos inmobiliarios no rurales y no residenciales",
    ),
    "Real Estate Loans 1-4 Family Charge-Offs": (
        "Baixas por perda de empréstimos imobiliários residenciais de 1 a 4 unidades",
        "Castigos por pérdida de préstamos inmobiliarios residenciales de 1 a 4 unidades",
    ),
    "Real Estate Loans 1-4 Family Chg-Offs-Qtr": (
        "Baixas por perda de empréstimos imobiliários residenciais de 1 a 4 unidades no trimestre",
        "Castigos por pérdida de préstamos inmobiliarios residenciales de 1 a 4 unidades del trimestre",
    ),
}
