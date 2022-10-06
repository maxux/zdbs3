import os
import sys
import time
import datetime
import uuid
import base64
import hashlib
import redis
import locale
import json
import xml.etree.ElementTree as et
from datetime import datetime
from config import config
from flask import Flask, request, make_response, abort, g, stream_with_context


class S3ZDB:
    def __init__(self):
        # needed for date format
        locale.setlocale(locale.LC_TIME, 'en_US.utf-8')

        self.app = Flask("zdbs3")
        self.routes()

        self.callbacks = {
            'HeadObject': self.HeadObject,
            'ListObjectsV2': self.ListObjectsV2,
            'GetObject': self.GetObject,
            'PutObject': self.PutObject,
            'PutBucket': self.PutBucket,
            'HeadBucket': self.HeadBucket,
            'ListBuckets': self.ListBuckets,
            'DeleteBucket': self.DeleteBucket,
            'ListObjectVersions': self.ListObjectVersions,
            'GetBucketObjectLockConfig': self.GetBucketObjectLockConfig,
            'GetBucketLocation': self.GetBucketLocation,
            'NewMultipartUpload': self.NewMultipartUpload,
            'PutObjectPart': self.PutObjectPart,
            'CompleteMultipartUpload': self.CompleteMultipartUpload,
        }






    def HeadObject(self, bucket, name):
        metadata = self.fetchmeta(bucket, name)

        keytime = g.zdb.execute_command("KEYTIME", name)

        response = make_response()
        response.headers['ETag'] = metadata['checksum']
        response.headers['Content-Type'] = 'image/png' # FIXME
        response.headers['Content-Length'] = metadata['size']
        response.headers['Last-Modified'] = self.datetime_http(keytime)

        return response

    def GetObject(self, bucket, name):
        metadata = self.fetchmeta(bucket, name)

        keytime = g.zdb.execute_command("KEYTIME", name)

        self.setdata(bucket)

        def stream(metadata):
            for chunk in metadata['chunks']:
                yield g.zdb.get(chunk)

        headers = {
            'Last-Modified': self.datetime_http(keytime)
        }

        return self.app.response_class(stream_with_context(stream(metadata)), headers=headers)

    def ListObjectsV2(self, bucket):
        """
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>hello</Name>
            <Prefix></Prefix>
            <KeyCount>2</KeyCount>
            <MaxKeys>1000</MaxKeys>
            <Delimiter>/</Delimiter>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>file-10m</Key>
                <LastModified>2022-10-05T22:25:47.026Z</LastModified>
                <ETag>&#34;2f901ab3d4b842ac89ee74d549cd023e&#34;</ETag>
                <Size>10485760</Size>
                <Owner>
                    <ID>02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4</ID>
                    <DisplayName>minio</DisplayName>
                </Owner>
                <StorageClass>STANDARD</StorageClass>
            </Contents>
            <Contents>
                <Key>magic-key-white.png</Key><LastModified>2022-10-05T01:13:25.058Z</LastModified><ETag>&#34;ad9af02d185192e459e3c5053cc547cc&#34;</ETag><Size>100509</Size><Owner><ID>02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4</ID><DisplayName>minio</DisplayName></Owner><StorageClass>STANDARD</StorageClass>
            </Contents>
            <EncodingType>url</EncodingType>
        </ListBucketResult>
        """

        root = self.replier('ListBucketResult')

        self.setmeta(bucket)

        self.append(root, 'Name', bucket)
        self.append(root, 'Prefix', "")
        self.append(root, 'KeyCount', "42")
        self.append(root, 'MaxKeys', "1000")
        self.append(root, 'Delimiter', "/")
        self.append(root, 'IsTruncated', "false")


        items = g.zdb.execute_command("SCAN")

        for entry in items[1]:
            contents = self.append(root, "Contents")

            metadata = self.fetchmeta(bucket, entry[0].decode('utf-8'))

            owner = self.append(contents, 'Owner')
            self.append(owner, 'ID', '02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4')
            self.append(owner, 'DisplayName', 'HelloWorld')

            self.append(contents, 'Key', entry[0].decode('utf-8'))
            self.append(contents, 'LastModified', '2022-10-05T22:25:47.026Z')
            self.append(contents, 'ETag', '42')
            self.append(contents, 'Size', str(metadata['size']))
            self.append(contents, 'StorageClass', 'STANDARD')

        return self.response(root)


    def PutObjectStreaming(self, bucket, name):
        payload = request.data
        realdata = b''

        metadata = {
            'chunks': [],
            'size': 0,
            'checksum': '',
        }

        chunkb2 = hashlib.blake2b(digest_size=20)
        fullb2 = hashlib.blake2b(digest_size=20)

        self.setdata(bucket)

        while True:
            delimiter = payload.find(b';')
            if delimiter == -1:
                break

            length = int(payload[:delimiter], base=16)
            head = delimiter + 17
            signature = payload[head:head + 64]

            # avoid any payload larger than 4 MB (minus some marging)
            if len(realdata) + length > ((4 * 1024 * 1024) - (512 * 1024)):
                chunkey = self.commit(realdata, chunkb2)
                metadata['chunks'].append(chunkey)

                chunkb2 = hashlib.blake2b(digest_size=20)
                realdata = b''

            data = payload[head + 64 + 2:head + 64 + 2 + length]

            realdata += data
            metadata['size'] += len(data)

            chunkb2.update(data)
            fullb2.update(data)

            # print(length, signature, len(data))

            payload = payload[head + 64 + 2 + length + 2:]

        chunkey = self.commit(realdata, chunkb2)
        metadata['chunks'].append(chunkey)

        self.setmeta(bucket)
        metadata['checksum'] = fullb2.hexdigest()

        g.zdb.execute_command("SET", name, json.dumps(metadata))

        response = make_response()
        response.headers['ETag'] = metadata['checksum']

        return response

    def PutObject(self, bucket, name):
        if request.headers.get("X-Amz-Content-Sha256") == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD":
            return self.PutObjectStreaming(bucket, name)

        return self.NotImplemented()

    def PutObjectPart(self, bucket, name):
        uploadid = request.args.get("uploadId")
        partnumber = request.args.get("partNumber")

        print(uploadid, partnumber)

        if request.headers.get("X-Amz-Content-Sha256") == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD":
            return self.PutObjectStreaming(bucket, f"{name}.{uploadid}.{partnumber}")

        return self.NotImplemented()

    def CompleteMultipartUpload(self, bucket, name):
        """
        <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <PartNumber>1</PartNumber>
                <ETag>f6c71056f43e095c4ecfe1d2d4eb8abd</ETag>
                <ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256>
            </Part>
            <Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>2</PartNumber><ETag>07bf3ef749019698a9581853fe004da7</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>3</PartNumber><ETag>171622967143a502002a6ed3b97feec8</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>4</PartNumber><ETag>58aca8063e20c770252cad14464060ea</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>5</PartNumber><ETag>cd50d5c733cf2b905106405d89d15db6</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>6</PartNumber><ETag>86fdc772e2be5804a3ebf537d8520207</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>7</PartNumber><ETag>570edfa76e670d7f8259558a2b32fde5</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>8</PartNumber><ETag>e9f3c00ccbeec28ccef510d4ab81a4dd</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>9</PartNumber><ETag>d04dda3747501e6d61a34830fa85bda0</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>10</PartNumber><ETag>1084a7e8e3ff8f02db2cc652dd16c46c</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>11</PartNumber><ETag>6d19a7f3a7d7049a28eb65b7c8b1b32b</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>12</PartNumber><ETag>29e6dd3189d214bbb306a19c145f7ca1</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>13</PartNumber><ETag>7be65bd12e1982fb758aef95971b137c</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>14</PartNumber><ETag>f84043bf2d015793c6a0fb4434005ff8</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>15</PartNumber><ETag>d27694ce238122e7fcad0a07eca16cd9</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>16</PartNumber><ETag>09ed73219cd241de2e756eea68dcd77d</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>17</PartNumber><ETag>8e082a9625eefe06dfa6f8d24c1cf196</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>18</PartNumber><ETag>11f4c5faa7a2f59770f8fc7f7f22ce9e</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>19</PartNumber><ETag>b647b1795662b80fcfde3ebd54db7655</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>20</PartNumber><ETag>b5a488058a777e345e4b5d52ef7518d2</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>21</PartNumber><ETag>aba88196fc508c8472625d5eeeb9d9be</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>22</PartNumber><ETag>7a70bc464feda327b2b66f5893208fe1</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>23</PartNumber><ETag>d004f1ddc602d130ea5e9e9760e6dbc2</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>24</PartNumber><ETag>6ca4f6bc0d3203174cd1abdf988a3c45</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>25</PartNumber><ETag>5f1d285b09e3df711872a0bb95caf66a</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>26</PartNumber><ETag>df9fb9b5dacf36cfda5768e6c8382084</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>27</PartNumber><ETag>f953e010e9653c18acd2018ca75b5391</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>28</PartNumber><ETag>6ec0a45f9d5f1c9b8c61a40189591dc4</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>29</PartNumber><ETag>45383fe756b398c628d3bdace8e2470b</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>30</PartNumber><ETag>bd49e0252d5e064d57d8b9b74b1d708b</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>31</PartNumber><ETag>a3386eda1ce225819ce42f3b4ddd7d44</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>32</PartNumber><ETag>b9da128682ccf1ebc6e2baded0c13245</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>33</PartNumber><ETag>42bf2f26939b5138f2eaef7554bd2c34</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>34</PartNumber><ETag>190dcac3fa922b8263035e218d04d6b9</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>35</PartNumber><ETag>fd58c5bb0e1408bcf8bc24f764660ceb</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>36</PartNumber><ETag>dad76b82187f8318e500815d2e947cc1</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>37</PartNumber><ETag>760438cd101b8b3c1e80aa284f67a235</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>38</PartNumber><ETag>30573617fceba0cb1fdcd15064c01c64</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>39</PartNumber><ETag>d3fb543cb369390174b4d11465b49132</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>40</PartNumber><ETag>f8c76afeca914b14405c1dcbf670c1a6</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>41</PartNumber><ETag>e5718bfbfafedab0c4a66d094c2c41fd</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>42</PartNumber><ETag>0127b96282f21b6d9443cc15c10f19e4</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>43</PartNumber><ETag>5f6ed39c0e63d2a2b7e92f49afced657</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>44</PartNumber><ETag>2f2cb63f451c292663e2d6f9ffce7eda</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>45</PartNumber><ETag>5c69f9100462c7b873f0b15f7d2f88e3</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>46</PartNumber><ETag>e98aed531ff2537b3639186b2b69bf23</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>47</PartNumber><ETag>20a91a08789d165714462055ff1199c6</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>48</PartNumber><ETag>98682559870e6b6ecd6e07f227e9948c</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>49</PartNumber><ETag>4a518123b003446692c77b0095c62436</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>50</PartNumber><ETag>5bac0aae194edfe44aeff82ed4419186</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>51</PartNumber><ETag>0b32660eb8579c741824d0e1430b14ba</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>52</PartNumber><ETag>761a522fe56eb552f08f8abcda3b92ab</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>53</PartNumber><ETag>397fd0eb7c95d683a46933bee14abb81</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>54</PartNumber><ETag>eaf1b82f948090080c2271209547e2ef</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>55</PartNumber><ETag>42b15dd7ac26975effbb7ddabb677274</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>56</PartNumber><ETag>005ebe6e54898b3c746ae6034f36004e</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>57</PartNumber><ETag>5989271713e5ebe507a34b507e4cba6e</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>58</PartNumber><ETag>869e0ead72f89f52f3de40f922f3f99d</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>59</PartNumber><ETag>4b90519dc01703dd9bb49285a15ac106</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>60</PartNumber><ETag>4ef3ba72f27fbf911aa18ba448fa80a0</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>61</PartNumber><ETag>5c9009275a02a309816ab2697265cbfb</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>62</PartNumber><ETag>4641d6b303f98818643e08e41a186009</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>63</PartNumber><ETag>eae7b03e329db42b49260ab2dd787954</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>64</PartNumber><ETag>35f08730d282c8e5be378cb97e8e71be</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>65</PartNumber><ETag>7165a096f8cfa33fcda397e9bfd2f76c</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>66</PartNumber><ETag>2709e74ee9851a396f5551a9a2faf639</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>67</PartNumber><ETag>1def8c6f5ab6e5e0329cfb9d1e416b10</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>68</PartNumber><ETag>909804736c556a9c0de9345dee32dd5b</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>69</PartNumber><ETag>00b9cdd42ecfd2b1266c41ccc307691e</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>70</PartNumber><ETag>0fa318061130f2e22be6cc008dac3b8e</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>71</PartNumber><ETag>b19f22982ae643ef354aeddacafac3ac</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>72</PartNumber><ETag>87c0be2394fe923838f0e9eb7593f2e9</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>73</PartNumber><ETag>8af46dce1653fc035f5c684b8b32d8eb</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>74</PartNumber><ETag>56f4babe32cbd8ae9e46cc200ba645dc</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>75</PartNumber><ETag>d15f37fcfe67115a2035252712984152</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>76</PartNumber><ETag>005c68da7eb976bd0547a174958cf70a</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>77</PartNumber><ETag>2c21fca18845b189962de899b9143e12</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>78</PartNumber><ETag>b02b44ce00b24b4761dce8195577142e</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>79</PartNumber><ETag>bfb09e80c5ebd6c6f4df839d12523faf</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>80</PartNumber><ETag>011ae5353f8bcba113112a2aa9e3d4fd</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>81</PartNumber><ETag>c464682f7e9748a2da44f403a0feee50</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>82</PartNumber><ETag>4f26bc1c465c98030509af2602d89768</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>83</PartNumber><ETag>6fbe0ddedfccc553f09ed8c0e3882e4e</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>84</PartNumber><ETag>027e76249921db73c6d15b10c356c3e6</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>85</PartNumber><ETag>0b0b87f36915f1922068abb536d71f70</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>86</PartNumber><ETag>8bd4622e10c7e4eb81ee26ae8a5abced</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>87</PartNumber><ETag>7515652adbedfeeed0f6bd91080186c4</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>88</PartNumber><ETag>eeecbe7ac5b4763b52bc001ff8224618</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>89</PartNumber><ETag>ac9b366df2932193a63c68d398ca08f8</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>90</PartNumber><ETag>39bc56f45550d56116d31d1fd78ae0ef</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>91</PartNumber><ETag>eb4692b550a8b1e1c1e9a7eaa919349e</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>92</PartNumber><ETag>5422b4778d99d3a762bf32c87a66695c</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>93</PartNumber><ETag>bf12e2ee3adbf80efaaae1b0e1c225a1</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>94</PartNumber><ETag>0f5cad0bc30760bfc4f1a9039eb8d3ba</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>95</PartNumber><ETag>d31ee2d8ea208d351de49abdb89c0f4a</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>96</PartNumber><ETag>42c151697c27ec1ded11cdab9dd04d44</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>97</PartNumber><ETag>cc35d2fe9bdb3c2e2c7de0c751212694</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>98</PartNumber><ETag>04263be75cd70e84dd3c21095b759e3e</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>99</PartNumber><ETag>4b1de7a6ce78692c3237a9fd92088736</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>100</PartNumber><ETag>fbf68fead9289a636aaf503bd82d9e2c</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>101</PartNumber><ETag>569bb6394113b0eff3ed52ba8342dd2c</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>102</PartNumber><ETag>8727f963e35561a4dce37f8150459652</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>103</PartNumber><ETag>74c1b36f4c821c5dedcc4c3786ea5c47</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>104</PartNumber><ETag>0377b2281d130f6dbd2ffe01268cda97</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>105</PartNumber><ETag>3ef0895643f303c2212ff9f3ca555ed7</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>106</PartNumber><ETag>09924fbf06fa611c684f6dc9faa04a42</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>107</PartNumber><ETag>2dd78e6f07f3e138f98dfc04aeb8553e</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>108</PartNumber><ETag>9d9da6fc68db8a0cd424ebb15b742d0f</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>109</PartNumber><ETag>23f2ca80b3b5dba4061b7a23c7a3256c</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>110</PartNumber><ETag>029de0fe2f16d36ff5a7a311f100e018</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>111</PartNumber><ETag>0f4b67d48c117028f5106facfe4416e2</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>112</PartNumber><ETag>71ff61ab70f1ec1619ec913c81c11a1b</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>113</PartNumber><ETag>ee3e0c7f64f4fa38e69a6e54449c1afd</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>114</PartNumber><ETag>0164fc3a805c3fbd614b28574cae7073</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>115</PartNumber><ETag>adbb7da600f34ab2eeac0a8a97777f38</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>116</PartNumber><ETag>c04ec1ff8247b978c089da6280c2782c</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>117</PartNumber><ETag>e2c944fb30c0f7c02bb72e4b0b5c139a</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>118</PartNumber><ETag>d4601d7e3f192e5328fc5d5a47478087</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>119</PartNumber><ETag>ff76dff49adcd1d9f10b21c0b9a68004</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>120</PartNumber><ETag>e88cc3bb0e123198517ed59036b195d3</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>121</PartNumber><ETag>312e62ce68b40a49f29d1e0da7f65582</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>122</PartNumber><ETag>5d1ebf0ebb491edd3bb5224f591ce861</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>123</PartNumber><ETag>b56f5f1986e529e10ff93bfc80619513</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>124</PartNumber><ETag>0c5a2038048140fd31ba99a288041d2f</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>125</PartNumber><ETag>c15689751088e7f87b81bd055b82a879</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>126</PartNumber><ETag>a70477458764b6ef88d28c2479ec6537</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>127</PartNumber><ETag>a62c71b9a309a4d8bac70b2fc9e92b98</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part><Part xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><PartNumber>128</PartNumber><ETag>bff3031382f077ec8eb34841cc9f1f12</ETag><ChecksumCRC32></ChecksumCRC32><ChecksumCRC32C></ChecksumCRC32C><ChecksumSHA1></ChecksumSHA1><ChecksumSHA256></ChecksumSHA256></Part>
        </CompleteMultipartUpload>

        <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Location>http://localhost:9000/hello/file-2g</Location>
            <Bucket>hello</Bucket>
            <Key>file-2g</Key>
            <ETag>&#34;c696a9be9713bb7efe83678cbc4efda1-128&#34;</ETag>
            <ChecksumCRC32></ChecksumCRC32>
            <ChecksumCRC32C></ChecksumCRC32C>
            <ChecksumSHA1></ChecksumSHA1>
            <ChecksumSHA256></ChecksumSHA256>
        </CompleteMultipartUploadResult>
        """

        uploadid = request.args.get("uploadId")
        self.setmeta(bucket)

        metadata = {
            'chunks': [],
            'size': 0,
            'checksum': '',
        }

        namespace = {'': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        partlist = et.fromstring(request.data)

        for part in partlist.findall('Part', namespace):
            for number in part.findall('PartNumber', namespace):
                # FIXME if not found
                """
                partmeta = g.zdb.execute_command("GET", f"{name}.{uploadid}.{number.text}")
                print(partmeta)
                partinfo = json.loads(partmeta)
                """
                partinfo = self.fetchmeta(bucket, f"{name}.{uploadid}.{number.text}")

                for chunk in partinfo['chunks']:
                    metadata['chunks'].append(chunk)

                metadata['size'] += partinfo['size']

        g.zdb.execute_command("SET", name, json.dumps(metadata))

        for part in partlist.findall('Part', namespace):
            for number in part.findall('PartNumber', namespace):
                g.zdb.execute_command("DEL", f"{name}.{uploadid}.{number.text}")

        root = self.replier('CompleteMultipartUploadResult')
        self.append(root, 'Location', 'http://')
        self.append(root, 'Bucket', bucket)
        self.append(root, 'Key', name)
        self.append(root, 'ETag', 'HelloWorld')
        self.append(root, 'ChecksumCRC32', '')
        self.append(root, 'ChecksumCRC32C', '')
        self.append(root, 'ChecksumSHA1', '')
        self.append(root, 'ChecksumSHA256', '')

        return self.response(root)

    def PutBucket(self, bucket):
        try:
            g.zdb.execute_command('NSNEW', f"meta.{bucket}")
            g.zdb.execute_command('NSNEW', f"data.{bucket}")

        except Exception as e:
            print(e)
            return self.error("NoSuchBucket", "Bucket not found", 404)

        return "OK"

    def HeadBucket(self, bucket):
        if self.exists(bucket):
            return "OK"

        return self.error("BucketNotFound", "Bucket not found", 404)

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

        nslist = g.zdb.execute_command('NSLIST')
        for ns in nslist:
            name = ns.decode('utf-8')

            if not name.startswith("meta."):
                continue

            bucket = self.append(buckets, 'Bucket')
            self.append(bucket, 'Name', name[5:])
            self.append(bucket, 'CreationDate', '2000-01-01T00:01:00.956Z')

        return self.response(root)

    def DeleteBucket(self, bucket):
        try:
            g.zdb.execute_command("NSDEL", f"meta.{bucket}")
            g.zdb.execute_command("NSDEL", f"data.{bucket}")

        except Exception as e:
            return self.error("NoSuchBucket", "Bucket not found", 404)

        return self.empty()

    def ListObjectVersions(self, bucket):
        return "OK"

    def GetBucketObjectLockConfig(self, bucket):
        return self.error("ObjectLockConfigurationNotFoundError", "Object Lock configuration does not exist for this bucket", 404)

    def GetBucketLocation(self, bucket):
        if not self.exists(bucket):
            return self.error("BucketNotFound", "Bucket does not exists", 404)

        root = self.replier("LocationConstraint")
        return self.response(root)

    def NewMultipartUpload(self, bucket, name):
        """
        <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Bucket>hello</Bucket>
            <Key>file-2g</Key>
            <UploadId>eea48151-42e6-4933-8c29-b052cba9833b</UploadId>
        </InitiateMultipartUploadResult>
        """
        root = self.replier('InitiateMultipartUploadResult')
        self.append(root, 'Bucket', bucket)
        self.append(root, 'Key', name)
        self.append(root, 'UploadId', 'XXXXX') # FIXME

        return self.response(root)


    def exists(self, bucket):
        try:
            g.zdb.execute_command("SELECT", f"meta.{bucket}")
            return True

        except Exception as e:
            return False

    def fetchmeta(self, bucket, name):
        self.setmeta(bucket)

        metadata = g.zdb.get(name)
        return json.loads(metadata.decode('utf-8'))

    def commit(self, payload, digest):
        chunkey = digest.hexdigest()

        print(f"[+] commit chunks: {chunkey}")
        g.zdb.execute_command("SET", chunkey, payload)

        return chunkey

    def setmeta(self, bucket):
        g.zdb.execute_command("SELECT", f"meta.{bucket}")

    def setdata(self, bucket):
        g.zdb.execute_command("SELECT", f"data.{bucket}")



    def datetime_http(self, timestamp):
        return datetime.utcfromtimestamp(timestamp).strftime('%a, %d %b %Y %H:%M:%S GMT')

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

        return self.callbacks[callname](**kwargs)

    def NotImplemented(self):
        abort(500)





    def routes(self):
        @self.app.before_request
        def before_request_handler():
            g.zdb = redis.Redis(port=9900)
            del g.zdb.response_callbacks['SCAN']
            del g.zdb.response_callbacks['DEL']
            # g.zdb.ping()

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

                if request.args.get("list-type") == "2":
                    return self.call('ListObjectsV2', bucket=bucket)

                return self.NotImplemented()

            if request.method == "HEAD":
                return self.call('HeadBucket', bucket=bucket)

            if request.method == "DELETE":
                return self.call('DeleteBucket', bucket=bucket)

            return self.NotImplemented()

        @self.app.route('/<bucket>/<name>', methods=['HEAD', 'GET', 'PUT', 'POST'])
        def route_bucket_object(bucket, name):
            if request.method == "HEAD":
                return self.call('HeadObject', bucket=bucket, name=name)

            if request.method == "GET":
                return self.call('GetObject', bucket=bucket, name=name)

            if request.method == "PUT":
                if request.args.get("uploadId", None) != None:
                    return self.call('PutObjectPart', bucket=bucket, name=name)

                return self.call('PutObject', bucket=bucket, name=name)

            if request.method == "POST":
                if request.args.get("uploads", None) != None:
                    return self.call('NewMultipartUpload', bucket=bucket, name=name)

                if request.args.get("uploadId", None) != None:
                    return self.call('CompleteMultipartUpload', bucket=bucket, name=name)

            return self.NotImplemented()




    def run(self):
        self.app.run(host=config['listen'], port=config['port'], debug=config['debug'], threaded=True)

if __name__ == '__main__':
    s = S3ZDB()
    s.run()
