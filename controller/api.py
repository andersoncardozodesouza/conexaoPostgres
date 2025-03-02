from flask import Flask, jsonify, request
from flask_cors import CORS

from service.despesasOrcadasEmpenhadasSevice import DespesasOrcadasEmpenhadasService
from model.despesasOrcadasEmpenhadas import DespesasOrcadasEmpenhadas


despesasOrcadasEmpenhadasService = DespesasOrcadasEmpenhadasService()

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

class Api:        

    @app.route('/api/despesasOrcadasEmpenhadas', methods=['GET'])
    def obter_despesassOrcadasEmpenhadas():
        idtce = request.args.getlist('idtce', type=str) 
        idquadrimestres = request.args.getlist('idquadrimestres', type=str)
        ano = request.args.getlist('ano', type=int)
        bancoEloJr = request.args.getlist('bancoEloJr', type=int)
        bancoEloWeb = request.args.getlist('bancoEloWeb', type=int)


        
        print(f"Entidades: {idtce}")
        print(f"ID Quadrimestres: {idquadrimestres}")
        print(f"Ano: {ano}")

        if not idtce:
            return jsonify({"error": "entidades são obrigatórios"}), 400 

        if not idquadrimestres:
            return jsonify({"error": "idquadrimestres são obrigatórios"}), 400
        if idquadrimestres == 1:
            idquadrimestres = request.args.getlist('idquadrimestres', type=int)
        else:
            idquadrimestres = request.args.getlist('idquadrimestres', type=str)
        
        if not ano:
            return jsonify({"error": "ano é obrigatório"}), 400

        try:
            despesas = despesasOrcadasEmpenhadasService.obter_despesasOrcadasEmpenhadasService(bancoEloJr, idtce, idquadrimestres, ano)
            print(despesas)
            if not despesas:
                raise ValueError("Nenhuma despesa encontrada.")
            else:                 
                despesas_a_inserir = [despesas(f"Despesa {i}", 1000 + i, 10.5) for i in range(1000)]
                despesasOrcadasEmpenhadasService.salvar_despesasOrcadasEmpenhadasService(bancoEloWeb, despesas_a_inserir)
            return jsonify(despesas), 200    
        except Exception as e:
            return jsonify({"error": str(e)}), 500   