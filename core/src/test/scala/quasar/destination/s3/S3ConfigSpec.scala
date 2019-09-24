/*
 * Copyright 2014–2019 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.destination.s3

import argonaut.{Argonaut, DecodeJson, Json}, Argonaut._
import org.specs2.mutable.Specification

object S3ConfigSpec extends Specification {
  "parses a virtual host-style bucket" >> {
    val sampleConfig = Json.obj(
      "bucket" := "https://some-bucket.s3-eu-west-1.amazonaws.com",
      "credentials" := Json.obj(
        "accessKey" := "key1",
        "secretKey" := "key2",
        "region" := "eu-west-1"))

    DecodeJson.of[S3Config].decodeJson(sampleConfig).toOption must beSome(
      S3Config("some-bucket", S3Credentials(
        AccessKey("key1"), SecretKey("key2"), Region("eu-west-1"))))
  }

  "parses a path-style bucket" >> {
    val sampleConfig = Json.obj(
      "bucket" := "https://s3-eu-west-1.amazonaws.com/some-bucket",
      "credentials" := Json.obj(
        "accessKey" := "key1",
        "secretKey" := "key2",
        "region" := "eu-west-1"))

    DecodeJson.of[S3Config].decodeJson(sampleConfig).toOption must beSome(
      S3Config("some-bucket", S3Credentials(
        AccessKey("key1"), SecretKey("key2"), Region("eu-west-1"))))
  }
}
