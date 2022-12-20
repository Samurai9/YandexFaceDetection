import boto3
import io
import base64
import requests
import json


def handler(event, context):

    aws_access_key_id=os.environ['aws_access_key_id']
    aws_secret_access_key=os.environ['aws_secret_access_key']

    bucket_id = event['messages'][0]['details']['bucket_id']
    object_id = event['messages'][0]['details']['object_id']

    session = boto3.session.Session()
    s3 = session.client(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        region_name='ru-central1'
    )
    img = io.BytesIO()
    s3.download_fileobj(bucket_id, object_id, img)

    encoded_img = base64.b64encode(img.getbuffer().tobytes())

    response = requests.post("https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze",
                        data=json.dumps({
                                "folderId": "b1gtp3olnb1cdk2as3jd",
                                "analyze_specs": [{
                                    "content": encoded_img.decode("UTF-8"),
                                    "features": [{
                                        "type": "FACE_DETECTION"
                                    }]
                                }]
                            }
                        ), 
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": "Bearer t1.9euelZrOkMaPjozGzseMy5yey8-ciu3rnpWanJ6Rm4qWkpjNycyQlo6SzJzl9PdNM3hi-e8aUy6n3fT3DWJ1YvnvGlMupw.jsvC1pyIkMRhwiz7wAlwSEe-CQxYg9GUy1YAFlvfZ3JZZ5KG0w_XUn1aKwrQIyYPx8QKwyNIkcH1Mip5J5lIAQ" 
                        })

    queue_client = boto3.client(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1'
    )

    queue_url = queue_client.get_queue_url(QueueName="vvot21-tasks")['QueueUrl']

    for face in response.json()['results'][0]['results'][0]['faceDetection']['faces']:
            queue_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({
                    'object_id': object_id,
                    'vertices': face['boundingBox']['vertices']
                })
            )

