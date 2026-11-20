# Atividade Avaliativa CDIA - Big Data

Entregar um pipeline em camadas usando:
- Spark;
- Cilo;
- Delta Lake.

O Delta Lake deve ser organizado em:
- Dados crus na **landing**;
- Dados ingeridos e rastreáveis na **bronze**;
- Dados limpos, validados e confiáveis na **silver**;
- Dados modelados para análise e BI na **gold**.

A geração de dados corrompidos encontra-se no arquivo notebook python `gerador-dados`.
