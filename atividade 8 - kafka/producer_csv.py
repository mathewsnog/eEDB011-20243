from kafka import KafkaProducer

sourcepathreclamacoes = r"C:\Users\teu20\Documents\Poli\Dados\Source\reclamacoes"

def produce_invoices():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    csv_file = r"C:\Users\teu20\Documents\Poli\Dados\Source\reclamacoes\2021_tri_01.csv"
    
    # Read the CSV file as a string
    with open(csv_file, 'r') as file:
        csv_content = file.read()

    # Send the CSV content as a message value
    producer.send('bancos_data', value=csv_content.encode('utf-8'))
    
    producer.flush()
    producer.close()

produce_invoices()