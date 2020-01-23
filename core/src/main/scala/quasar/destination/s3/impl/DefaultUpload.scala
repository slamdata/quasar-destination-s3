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

package quasar.destination.s3.impl

import quasar.destination.s3.Upload
import quasar.blobstore.s3.{Bucket, S3PutService}
import quasar.blobstore.paths.BlobPath

import slamdata.Predef._

import cats.effect.{ContextShift, Concurrent, Timer}
import cats.implicits._

import fs2.Stream

import software.amazon.awssdk.services.s3.S3AsyncClient

final case class DefaultUpload[F[_]: Concurrent: ContextShift: Timer](client: S3AsyncClient, partSize: Int)
    extends Upload[F] {

  def upload(bytes: Stream[F, Byte], bucket: Bucket, key: BlobPath): Stream[F, Unit] = {
    val service = S3PutService[F](client, partSize, bucket)

    Stream.eval(service((key, bytes))).void
  }
}
