from google.cloud import bigquery

# tạo client
client = bigquery.Client()

print("Connected to BigQuery!")

# list datasets trong project
datasets = list(client.list_datasets())

if datasets:
    print("Datasets in project:")
    for dataset in datasets:
        print(dataset.dataset_id)
else:
    print("No datasets found.")