import pandas as pd
import time

# Extração de Dados
## Vamos carregar os dados bruto da fonte de dados

file_path = r"C:\Users\Washington\Documents\Iago M\Projeto ETL- Campanha Meta Ads\meta_ads_data.csv"

df = pd.read_csv(file_path)

print(df.head())
print('\n\n\n\n\n')

# Transformação
## Queremos limpar, organixar e calcular métricas para preparalos para a análise.
## Vamos remover dados inconsistentes, criar novas colunas e flitrar campanhas com baixo desempenho.

# Removamos dados nulos (Caso tenha, mas quase sempre terá)

df.dropna(inplace=True)

df['date'] = pd.to_datetime(df['date'])

# Criando a coluna de custo por conversão -> CPA, Custo por Aquisição de Cliente
df['cpa'] = df['cost'] / df['conversions']

# Tratamos valores infinitos, como 0, o que atrapalharia a métrica
df.replace([float('inf'), -float('inf')], 0, inplace=True)

# Removemos as campanhas com menos de 1000 views
df = df[df['impressions'] >= 1000]

print(df.head())

# Caminho do novo arquivo CSV com dados transformados
output_path = r"C:\Users\Washington\Documents\Iago M\Projeto ETL- Campanha Meta Ads\meta_ads_data_transformed.csv"

# Salvar o DataFrame transformado em um novo CSV
df.to_csv(output_path, index=False)

print(f"Dados transformados salvos com sucesso em: {output_path}")


print('\n\n\n\n')

# Carregando estes dados para o MySQL no PhpMyAdmin

import mysql.connector
import pandas as pd

# Conectar ao MySQL
conn = mysql.connector.connect(
    host="localhost",       # Pode ser 'localhost' ou o IP do seu servidor
    user="root",            # Seu usuário do MySQL
    password="",   # Sua senha do MySQL
    database="campanhas_meta"     # Nome do banco de dados que você criou
)

cursor = conn.cursor()

# Carregar o novo DF com dados transformados
df = pd.read_csv(r"C:\Users\Washington\Documents\Iago M\Projeto ETL- Campanha Meta Ads\meta_ads_data_transformed.csv")

# Inserir dados na tabela 'campanhas_meta'
for i, row in df.iterrows():
    sql = """
    INSERT INTO campanhas_meta (ad_id, date, impressions, clicks, cost, conversions, ctr, cost_per_conversion)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
    impressions = VALUES(impressions),
    clicks = VALUES(clicks),
    cost = VALUES(cost),
    conversions = VALUES(conversions),
    ctr = VALUES(ctr),
    cost_per_conversion = VALUES(cost_per_conversion)
    """

    cursor.execute(sql, tuple(row))

# Confirmar as alterações
conn.commit()

# Fechar a conexão
cursor.close()
conn.close()

print("Dados carregados com sucesso no MySQL!")

