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

### Arquivos

- `gerador-dados.ipynb`: notebook de geração dos dados;
- `docker-compose.yml`: docker compose para subir os containers necessários para o projeto;
- `main.ipynb`: trabalho final;
- `ai_resume.md`: resumo criado pelo Gemini sobre o trabalho inteiro;

### Passo a passo de inicialização

1. Subir os containers docker;
2. Executar `gerador-dados.ipynb`;
3. Executar `main.ipynb`.
