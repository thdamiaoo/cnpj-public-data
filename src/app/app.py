from flask import Flask, render_template, request 
import pandas as pd

import sys
sys.path.append("..")

from database.database import Database

app = Flask(__name__)

@app.route('/filtro', methods=['GET', 'POST'])
def filtro():
    # Se o método for POST, aplicamos os filtros e exibimos o resultado
    if request.method == 'POST':
        # Obtemos os valores do formulário
        cnpj = request.form['cnpj']
        cnae_principal = request.form['cnae_principal']
        uf = request.form['uf']
        pais = request.form['pais']

        # Lemos os dados da tabela fato do banco de dados usando a classe Database
        df = Database().dados_fato()
        
        df_filtrado = df[(df['CNPJ'].str.contains(str(cnpj), na=False)) &
                         (df['CNAE'].str.contains(cnae_principal, na=False)) &
                         (df['UF'].str.contains(uf, na=False)) &
                         (df['País'].str.contains(pais, na=False))]

        # Renderizamos o template com o dataframe filtrado
        return render_template('resultado.html', tabela=df_filtrado.to_html())

    # Se o método for GET, exibimos o formulário
    return render_template('filtro.html')

if __name__ == '__main__':
    app.run(debug=True)
