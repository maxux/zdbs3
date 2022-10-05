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
        self.routes()

        self.zdb = redis.Redis(port=9900)
        self.zdb.ping()

        self.callbacks = {
            'HeadObject': self.HeadObject,
            'GetObject': self.GetObject,
            'PutObject': self.PutObject,
            'PutBucket': self.PutBucket,
            'HeadBucket': self.HeadBucket,
            'ListBuckets': self.ListBuckets,
            'DeleteBucket': self.DeleteBucket,
            'ListObjectVersions': self.ListObjectVersions,
            'GetBucketObjectLockConfig': self.GetBucketObjectLockConfig,
            'GetBucketLocation': self.GetBucketLocation,
        }






    def HeadObject(self, bucket, name):
        response = make_response()
        response.headers['ETag'] = 'aaaa'
        response.headers['Content-Type'] = 'image/png'
        response.headers['Content-Length'] = '100509' # FIXME
        response.headers['Last-Modified'] = 'Wed, 05 Oct 2022 00:46:15 GMT'

        return response

    def GetObject(self, bucket, name):
        data = self.zdb.get(name)
        response = make_response(data)
        response.headers['Last-Modified'] = 'Wed, 05 Oct 2022 00:46:15 GMT'

        return response

    def PutObject(self, bucket, name):
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

    def PutBucket(self, bucket):
        try:
            self.zdb.execute_command('NSNEW', bucket)

        except Exception as e:
            return self.error("NoSuchBucket", "Bucket not found", 404)

        return "OK"

    def HeadBucket(self, bucket):
        return "OK"

    def ListBuckets(self):
        root = self.replier('ListAllMyBucketsResult')

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

        owner = self.append(root, 'Owner')
        self.append(owner, 'ID', '02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4')
        self.append(owner, 'DisplayName', 'HelloWorld')

        buckets = self.append(root, 'Buckets')

        nslist = self.zdb.execute_command('NSLIST')
        for ns in nslist:
            bucket = self.append(buckets, 'Bucket')
            self.append(bucket, 'Name', ns.decode('utf-8'))
            self.append(bucket, 'CreationDate', '2000-10-04T23:01:35.956Z')

        return self.response(root)

    def DeleteBucket(self, bucket):
        try:
            self.zdb.execute_command("NSDEL", bucket)

        except Exception as e:
            return self.error("NoSuchBucket", "Bucket not found", 404)

        return self.empty()

    def ListObjectVersions(self, bucket):
        return "OK"

    def GetBucketObjectLockConfig(self, bucket):
        return self.error("ObjectLockConfigurationNotFoundError", "Object Lock configuration does not exist for this bucket", 404)

    def GetBucketLocation(self, bucket):
        root = self.replier("LocationConstraint")
        return self.response(root)





    def empty(self):
        return self.app.response_class("", status=204)

    def response(self, root, status=200):
        return self.app.response_class(et.tostring(root), mimetype='application/xml', status=status)

    def replier(self, name):
        return et.Element(name, {'xmlns': 'http://s3.amazonaws.com/doc/2006-03-01/'})

    def append(self, root, name, value=None):
        item = et.SubElement(root, name)

        if value != None:
            item.text = value

        return item

    def error(self, code, message, status=500):
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

        root = self.replier('Error')
        self.append(root, 'Code', code)
        self.append(root, 'Message', message)

        return self.response(root, status)






    def call(self, callname, **kwargs):
        print(f"[+] dispatch call: {callname}")
        print(kwargs)

        return self.callbacks[callname](*kwargs)

    def NotImplemented(self):
        abort(500)





    def routes(self):
        @self.app.route('/', methods=['GET'])
        def route_default():
            return self.call('ListBuckets')

        @self.app.route('/<bucket>/', methods=['HEAD', 'GET', 'PUT', 'DELETE'])
        def route_buckets(bucket):
            if request.method == "PUT":
                return self.call('PutBucket', bucket=bucket)

            if request.method == "GET":
                if request.args.get("location", None) != None:
                    return self.call('GetBucketLocation', bucket=bucket)

                if request.args.get("versions", None) != None:
                    return self.call('ListObjectVersions', bucket=bucket)

                if request.args.get("object-lock", None) != None:
                    return self.call('GetBucketObjectLockConfig', bucket=bucket)

                return self.NotImplemented()

            if request.method == "HEAD":
                return self.call('HeadBucket', bucket=bucket)

            if request.method == "DELETE":
                return self.call('DeleteBucket', bucket=bucket)

            return self.NotImplemented()

        @self.app.route('/<bucket>/<name>', methods=['HEAD', 'GET', 'PUT'])
        def route_bucket_object(bucket, name):
            if request.method == "HEAD":
                return self.call('HeadObject', bucket=bucket, name=name)

            if request.method == "GET":
                return self.call('GetObject', bucket=bucket, name=name)

            if request.method == "PUT":
                return self.call('PutObject', bucket=bucket, name=name)

            return self.NotImplemented()




    def run(self):
        self.app.run(host=config['listen'], port=config['port'], debug=config['debug'], threaded=True)

if __name__ == '__main__':
    s = S3ZDB()
    s.run()
