import boto3
import time

# Running jobs AWS Glue
job_names = ["glue_script", "nome_job2", "nome_job3"]

# Crie um cliente do AWS Glue
client = boto3.client("glue")

# Função para executar um job e aguardar o status


def run_job(job_name):
    response = client.start_job_run(JobName=job_name)
    job_run_id = response["JobRunId"]

    while True:
        response = client.get_job_run(JobName=job_name, RunId=job_run_id)
        status = response["JobRun"]["JobRunState"]
        print(f"Status do job '{job_name}': {status}")

        if status == "FAILED":
            error_message = response["JobRun"]["ErrorMessage"]
            return ("FAILED", error_message)
        elif status in ["SUCCEEDED", "STOPPED"]:
            return (status, None)

        # Aguarde um período de tempo antes de verificar o status novamente
        time.sleep(20)

overall_status = "SUCCEEDED"
error_description = None


for job_name in job_names:
    job_status, error_message = run_job(job_name)
    
    if job_status != "SUCCEEDED":
        overall_status = "FAILED"
        error_description = error_message
        break

print(f"Status geral: {overall_status}")

if error_description is not None:
    print(f"Erro: {error_description}")
