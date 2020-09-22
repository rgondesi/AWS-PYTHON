import boto3 as b3
import botocore as b4
import csv

def lambda_handler(event, context):
    BUCKET_NAME = 'raveendhar-bucket'
    S3_FILE_NAME = 'CustomerDatas.csv'

    filepath = '/tmp/' + S3_FILE_NAME

    json_data = [{"first_name":"James", "last_name":"Butterburg", "address": {"street": "6649 N Blue Gum St", "city": "New Orleans","state": "LA", "zip": "70116" }}]

    with open(filepath, "w") as file:
        csv_file = csv.writer(file)
        csv_file.writerow(["first_name","last_name","address"])
        for item in json_data:
            csv_file.writerow([item.get('first_name'), item.get('last_name'), item.get('address') ])
    
    csv_binary = open(filepath, 'rb').read()

    try:
        s3 = b3.resource('s3')
        obj = s3.Object(BUCKET_NAME, S3_FILE_NAME)
        obj.put(Body=csv_binary)
    except b4.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("Object doesn't exist")
        else:
            raise

    s3client = b3.client('s3')

    try:
        downloadable_url = s3client.generate_presigned_url(
                         'get_object',
                          Params={
                              'Bucket': BUCKET_NAME,
                              'Key': S3_FILE_NAME
                              },
                          ExpiresIn=3600
        )
        return {"csv_link": downloadable_url}
    except Exception as e:
        raise util_exception.ErrorResponse(400, e, Log)