# quasar-destination-s3 [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.precog" %% "quasar-destination-s3" % <version>
```

## Configuration

```json
{
  "bucket": String,
  "credentials": {
    "accessKey": String,
    "secretKey": String,
    "region": String
   }
}
```

- `bucket` is the full URL for the S3 bucket
- `accessKey` is your access key ID
- `secretKey` is your secret access key
- `region` is the code for the AWS region for the bucket e.g `us-west-2`

All fields are mandatory.
