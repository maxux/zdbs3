import os
import sys
import time
import datetime
import uuid
import base64
import hashlib
import redis
import xml.etree.ElementTree as et
from config import config
from flask import Flask, request, make_response, abort


class S3ZDB:
    def __init__(self):
        self.app = Flask("zdbs3")
        self.routes(self.app)
        self.zdb = redis.Redis(port=9900)
        self.zdb.ping()


    def routes(self, app):
        def empty(app):
            return app.response_class("", status=204)

        def response(app, root, status=200):
            return app.response_class(et.tostring(root), mimetype='application/xml', status=status)

        def replier(name):
            return et.Element(name, {'xmlns': 'http://s3.amazonaws.com/doc/2006-03-01/'})

        def append(root, name, value=None):
            item = et.SubElement(root, name)

            if value != None:
                item.text = value

            return item

        def error(app, code, message, status=500):
            """
            <Error>
                <Code>NoSuchBucket</Code>
                <Message>The specified bucket does not exist</Message>
                <BucketName>hello</BucketName>
                <Resource>/hello/</Resource>
                <RequestId>171B045355107080</RequestId>
                <HostId>a41a5ef2-f809-4e96-bf6d-09a9943109a7</HostId>
            </Error>
            """

            root = replier('Error')
            append(root, 'Code', code)
            append(root, 'Message', message)

            return response(app, root, status)

        @app.route('/', methods=['GET'])
        def index():
            root = replier('ListAllMyBucketsResult')

            """
            <?xml version="1.0" encoding="UTF-8"?>
            <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Owner>
                    <ID>02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4</ID>
                    <DisplayName>minio</DisplayName>
                </Owner>
                <Buckets>
                    <Bucket>
                        <Name>hello</Name>
                        <CreationDate>2022-10-04T22:48:00.866Z</CreationDate>
                    </Bucket>
                    <Bucket>
                        <Name>hellox</Name>
                        <CreationDate>2022-10-04T23:01:35.956Z</CreationDate>
                    </Bucket>
                </Buckets>
            </ListAllMyBucketsResult>
            """

            owner = append(root, 'Owner')
            append(owner, 'ID', '02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4')
            append(owner, 'DisplayName', 'HelloWorld')

            buckets = append(root, 'Buckets')

            nslist = self.zdb.execute_command('NSLIST')
            for ns in nslist:
                bucket = append(buckets, 'Bucket')
                append(bucket, 'Name', ns.decode('utf-8'))
                append(bucket, 'CreationDate', '2000-10-04T23:01:35.956Z')

            return response(app, root)

        @app.route('/<bucket>/', methods=['HEAD', 'GET', 'PUT', 'DELETE'])
        def buckets(bucket):
            if request.method == "PUT":
                print(f"MK BUCKET: {bucket}")
                try:
                    self.zdb.execute_command('NSNEW', bucket)

                except Exception as e:
                    return error(app, "NoSuchBucket", "Bucket not found", 404)

            if request.method == "GET":
                print(f"MK GET BUCKETS: {bucket}")
                if request.args.get("location", None) != None:
                    print("request location")
                    root = replier("LocationConstraint")
                    return response(app, root)

                if request.args.get("versions", None) != None:
                    # ListVersion
                    return "OK"

                if request.args.get("object-lock", None) != None:
                    # GetBucketObjectLockConfig
                    return error(app, "ObjectLockConfigurationNotFoundError", "Object Lock configuration does not exist for this bucket", 404)

                return "OK"

            if request.method == "HEAD":
                # Check if exists
                return "OK"

            if request.method == "DELETE":
                try:
                    self.zdb.execute_command("NSDEL", bucket)

                except Exception as e:
                    return error(app, "NoSuchBucket", "Bucket not found", 404)

                return empty(app)

            return "OK"

        @app.route('/<bucket>/<name>', methods=['HEAD', 'GET', 'PUT'])
        def buckets_object(bucket, name):
            if request.method == "HEAD":
                response = make_response()
                response.headers['ETag'] = 'aaaa'
                response.headers['Content-Type'] = 'image/png'
                response.headers['Content-Length'] = '100509' # FIXME
                response.headers['Last-Modified'] = 'Wed, 05 Oct 2022 00:46:15 GMT'

                return response

            if request.method == "GET":
                data = self.zdb.get(name)
                response = make_response(data)
                response.headers['Last-Modified'] = 'Wed, 05 Oct 2022 00:46:15 GMT'

                return response

            if request.method == "PUT":
                if request.headers.get("X-Amz-Content-Sha256") == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD":
                    print("STREAMING FEATURE")

                    payload = request.data
                    realdata = b''

                    while True:
                        delimiter = payload.find(b';')
                        if delimiter == -1:
                            break

                        length = int(payload[:delimiter], base=16)
                        head = delimiter + 17
                        signature = payload[head:head + 64]

                        data = payload[head + 64 + 2:head + 64 + 2 + length]
                        realdata += data

                        print(length, signature, len(data))

                        payload = payload[head + 64 + 2 + length + 2:]

                print(len(realdata))
                self.zdb.set(name, realdata)

                return "OK"

            return "OK"

    def run(self):
        self.app.run(host=config['listen'], port=config['port'], debug=config['debug'], threaded=True)

if __name__ == '__main__':
    s = S3ZDB()
    s.run()
