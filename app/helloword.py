from flask import Flask
from kafka import KafkaProducer
import logging
import json

def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['pkc-921jm.us-east-2.aws.confluent.cloud:9092'],
        value_serializer=lambda m: json.dumps(m).encode('ascii'),
        retry_backoff_ms=500,
        request_timeout_ms=20000,
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username='KDTDEMIDAKBAWYS4',
        sasl_plain_password='J2ffd7/grJWHDHETNGmODynDmL3bWk1sHZIXjKXRWU0ZCFw5Jrr5ebrVmN/Y4kJ1')

# saudação
def say_hello(username = "World"):
    # gravar o evento de forma assíncrona
    producer.send('webapp', {'says-hello' : username})
    return '<p>Hello %s!</p>\n' % username


logging.basicConfig(level=logging.DEBUG)

# alguns pedaços de texto para a página.
header_text = '''
    <html>\n<head> <title>Flask and Kafka</title> </head>\n<body>'''
instructions = '''
    <p><em>Hint</em>: Isto é uma fila! Coloque seu so</p>\n'''
home_link = '<p><a href="/">Back</a></p>\n'
footer_text = '</body>\n</html>'

# EB procura por um 'aplicativo' que pode ser chamado por padrão.
application = Flask(__name__)

# Cria um producer
producer=get_kafka_producer()

# #adicione uma regra para a página de índice.
application.add_url_rule('/', 'index', (lambda: header_text +
    say_hello() + instructions + footer_text))

#adiciona uma regra quando a página é acessada com um nome anexado ao site
# URL.
application.add_url_rule('/<username>', 'hello', (lambda username:
    header_text + say_hello(username) + home_link + footer_text))

# run the app.
if __name__ == "__main__":
    #Definir debug como True habilita a saída de depuração. Esta linha deve ser
    # removido antes de implantar um aplicativo de produção
    application.debug = True
    application.run()