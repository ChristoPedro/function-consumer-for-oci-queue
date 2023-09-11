import io
import json
import mysql.connector
import base64
from fdk import response

import oci.object_storage

def handler(ctx, data: io.BytesIO=None):
    try:
        cfg = dict(ctx.Config())
        messages = json.loads(data.getvalue())
        secret_ocid = cfg["secret_ocid"]
        host = cfg["host"]
        user = cfg["user"]

    except Exception:
        error = "insuficient parameters"
        raise Exception(error)
    resp = process_messages(messages, host, user, secret_ocid)
    batch_response = {}
    batch_response["batchItemFailures"] = resp
    return response.Response(
        ctx,
        response_data=json.dumps(batch_response),
        headers={"Content-Type": "application/json"}
    )

def process_messages(messages, host, user, secret_ocid):
   
    passwd = get_text_secret(secret_ocid)
    try:
        db = mysql_connect(host, user ,passwd)
    except Exception ex:
            raise ex
    batchItemFailures =[]
    for message in messages:
        try:
            insert_data(db, message)
        except Exception as ex:
            batchItemFailures.append(message['id'])
            raise ex
    return batchItemFailures

def get_text_secret(secret_ocid):
    try:
        signer = oci.auth.signers.get_resource_principals_signer()
        client = oci.secrets.SecretsClient(config={}, signer=signer)
        secret_content = client.get_secret_bundle(secret_ocid).data.secret_bundle_content.content.encode('utf-8')
        decrypted_secret_content = base64.b64decode(secret_content).decode("utf-8")
    except Exception as ex:
        print("ERROR: failed to retrieve the secret content", ex, flush=True)
        raise
    return decrypted_secret_content

def mysql_connect(host, user ,pw):
    try:
        mydb = mysql.connector.connect(
            host= host,
            port = 3306,
            user= user,
            password=pw)
    except Exception as ex:
        return ex
    return mydb

def insert_data(mydb, message):
    try:
        mycursor = mydb.cursor()
        sql = f"insert into otel_demo.Dados (dados) values  ('{json.dumps(message)}') "
        mycursor.execute(sql)
        mydb.commit()
    except Exception as ex:
        print("Error DB", ex, flush=True)
        raise