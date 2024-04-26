from minio import Minio
import os

# Création du client Minio
client = Minio(
    "localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

# Définition du répertoire local contenant les fichiers Parquet
local_dir = "/Users/mariam/Desktop/sandbox/tpPython/elem"

# Récupération de la liste des fichiers Parquet dans le répertoire local
parquet_files = [f for f in os.listdir(local_dir) if f.endswith('.parquet')]

# Nom du bucket sur Minio
bucket_name = "warehouse"

# Vérification si le bucket existe, sinon création
found = client.bucket_exists(bucket_name)
if not found:
    client.make_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' créé avec succès")
else:
    print(f"Bucket '{bucket_name}' existe déjà")

# Téléverser chaque fichier Parquet dans Minio
for file_name in parquet_files:
    local_file_path = os.path.join(local_dir, file_name)
    remote_file_path = f"elem/{file_name}"  # Nom du fichier sur Minio

    # Téléverser le fichier dans Minio
    client.fput_object(bucket_name, remote_file_path, local_file_path)
    print(f"Fichier '{file_name}' téléversé avec succès dans Minio")