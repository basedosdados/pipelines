# Documentação ME/CNPJ

Documento de registro de informações importantes sobre a base e relato sobre o estado de estruturação da pipeline, últimas mudanças e entendimento dos _flows_.

---

## Sobre a fonte

- [Fonte de Dados]()
- [Página Inicial do Conjunto]()

#### Tabelas Segmentadas

Tabelas que devem se baixadas em 10 arquivos diferentes:
- Empresas
- Estabelesimentos
- Socios

#### Tabelas Únicas
- Cnaes
- Motivos
- Qualificação

## Dicionário
<table style="border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;color:#000;">
  <thead>
    <tr>
      <th colspan="2" style="background-color:#93c47d; border:1px solid #000; padding:8px; text-align:center;">
        Colunas cobertas por Dicionário do Dataset
      </th>
      <th rowspan="2" style="background-color:#93c47d; border:1px solid #000; padding:8px; text-align:center;">
        Tabela Estruturada na Fonte
      </th>
      <th colspan="3" style="background-color:#93c47d; border:1px solid #000; padding:8px; text-align:center;">
        Colunas Ausentes no Dicionário do Dataset
      </th>
      <th rowspan="2" style="background-color:#93c47d; border:1px solid #000; padding:8px; text-align:center;">
        Diretórios
      </th>
    </tr>
    <tr>
      <th style="background-color:#93c47d; border:1px solid #000; padding:8px;">
        Tabela
      </th>
      <th style="background-color:#93c47d; border:1px solid #000; padding:8px;">
        Nome da Coluna
      </th>
      <th style="background-color:#93c47d; border:1px solid #000; padding:8px;">
        Tabela na Fonte Original
      </th>
      <th style="background-color:#93c47d; border:1px solid #000; padding:8px;">
        Tabela
      </th>
      <th style="background-color:#93c47d; border:1px solid #000; padding:8px;">
        Nome da Coluna
      </th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">empresas</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">porte</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px; text-align:center;">FALSE</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">-</td>
    </tr>
    <tr>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">empresas</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">qualificacao_responsavel</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px; text-align:center;">TRUE</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">Qualificacoes</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">-</td>
    </tr>
    <tr>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">estabelecimento</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">id_pais</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px; text-align:center;">TRUE</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">Paises</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">
        basedosdados.br_bd_diretorios_mundo.pais:nome
      </td>
    </tr>
    <tr>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">estabelecimento</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">identificador_matriz_filial</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px; text-align:center;">FALSE</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">-</td>
    </tr>
    <tr>
    <tr>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">estabelecimento</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">motivo_situacao_cadastral</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px; text-align:center;">TRUE</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">Motivos</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">-</td>
    </tr>
    <tr>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">estabelecimento</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">situacao_cadastral</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px; text-align:center;">FALSE</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">-</td>
    </tr>
    <tr>
    <tr>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">socios</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">faixa_etaria</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px; text-align:center;">FALSE</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">-</td>
    </tr>
    <tr>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">socios</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">id_pais</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px; text-align:center;">TRUE</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">Paises</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">
        basedosdados.br_bd_diretorios_mundo.pais:nome
      </td>
    </tr>
    <tr>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">socios</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">qualificacao</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px; text-align:center;">TRUE</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">Qualificacoes</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">-</td>
    </tr>
    <tr>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">socios</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">qualificacao_representante_legal</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px; text-align:center;">TRUE</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">Qualificacoes</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">-</td>
    </tr>
    <tr>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">socios</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">tipo</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px; text-align:center;">FALSE</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#b6d7a8; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">-</td>
    </tr>
    <tr>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px; text-align:center;">TRUE</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">Cnaes</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">estabelecimentos</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">cnae_fiscal_principal</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">
        basedosdados.br_bd_diretorios_brasil.cnae_2:subclasse
      </td>
    </tr>
    </tr>
    <tr>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px; text-align:center;">TRUE</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">Cnaes</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">estabelecimentos</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">cnae_fiscal_secundaria</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">
        basedosdados.br_bd_diretorios_brasil.cnae_2:subclasse
      </td>
    </tr>
  </tbody>
    </tr>
    <tr>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px; text-align:center;">TRUE</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">Naturezas</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">empresas</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">natureza_juridica</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">
        basedosdados.br_bd_diretorios_brasil.natureza_juridica:id_natureza_juridica
      </td>
    </tr>
    </tr>
    <tr>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px; text-align:center;">TRUE</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">Municipios</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#d9ead3; border:1px solid #000; padding:6px;">-</td>
      <td style="background-color:#ffffff; border:1px solid #000; padding:6px;">
        basedosdados.br_bd_diretorios_brasil.municipio:id_municipio_rf
      </td>
    </tr>
</table>
