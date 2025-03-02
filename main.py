import controller.api

if __name__ == '__main__':
    # Executa o servidor Flask na porta 5000 (ou qualquer outra que você escolher)
    controller.api.app.run(debug=True,port=3000)