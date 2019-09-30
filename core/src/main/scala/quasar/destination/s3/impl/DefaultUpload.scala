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

import quasar.destination.s3.{Bucket, ObjectKey, Upload}

import slamdata.Predef._

import cats.effect.{ContextShift, Concurrent, ExitCase}
import cats.effect.syntax.bracket._
import cats.syntax.functor._

import fs2.Stream

import monix.catnap.syntax._

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{
  AbortMultipartUploadRequest,
  AbortMultipartUploadResponse,
  CompleteMultipartUploadRequest,
  CompleteMultipartUploadResponse,
  CompletedMultipartUpload,
  CompletedPart,
  CreateMultipartUploadRequest,
  CreateMultipartUploadResponse,
  UploadPartRequest
}

final case class DefaultUpload[F[_]: Concurrent: ContextShift](client: S3AsyncClient, partSize: Int)
    extends Upload[F] {

  def upload(bytes: Stream[F, Byte], bucket: Bucket, key: ObjectKey): Stream[F, Unit] =
    Stream.bracketCase(startUpload(client, bucket, key)) {
      case (createResponse, ExitCase.Canceled | ExitCase.Error(_)) =>
        abortUpload(client, createResponse.uploadId, bucket, key).void
      case (_, ExitCase.Completed) =>
        Concurrent[F].unit
    } flatMap (createResponse =>
      for {
        parts <- uploadParts(client, bytes, createResponse.uploadId, partSize, bucket, key)
        _ <- Stream.eval(completeUpload(client, createResponse.uploadId, bucket, key, parts))
      } yield ())

  private def startUpload(
    client: S3AsyncClient,
    bucket: Bucket,
    key: ObjectKey): F[CreateMultipartUploadResponse] =
    Concurrent[F]
      .delay(
        client.createMultipartUpload(
          CreateMultipartUploadRequest.builder.bucket(bucket.value).key(key.value).build))
      .futureLift
      .guarantee(ContextShift[F].shift)

  private def uploadParts(
    client: S3AsyncClient,
    bytes: Stream[F, Byte],
    uploadId: String,
    minChunkSize: Int,
    bucket: Bucket,
    key: ObjectKey): Stream[F, List[CompletedPart]] =
    (bytes.chunkMin(minChunkSize).zipWithIndex evalMap {
      case (byteChunk, n) => {
        // parts numbers must start at 1
        val partNumber = Int.box(n.toInt + 1)

        val uploadPartRequest =
          UploadPartRequest.builder
            .bucket(bucket.value)
            .uploadId(uploadId)
            .key(key.value)
            .partNumber(partNumber)
            .contentLength(Long.box(byteChunk.size.toLong))
            .build

        val uploadPartResponse =
          Concurrent[F]
            .delay(
              client.uploadPart(
                uploadPartRequest,
                AsyncRequestBody.fromByteBuffer(byteChunk.toByteBuffer)))
            .futureLift
            .guarantee(ContextShift[F].shift)

        uploadPartResponse.map(response =>
          CompletedPart.builder.partNumber(partNumber).eTag(response.eTag).build)
      }
    }).fold(List.empty[CompletedPart])((acc, partResponse) => acc :+ partResponse)

  private def completeUpload(
    client: S3AsyncClient,
    uploadId: String,
    bucket: Bucket,
    key: ObjectKey,
    parts: List[CompletedPart]): F[CompleteMultipartUploadResponse] = {
    val multipartUpload =
      CompletedMultipartUpload.builder.parts(parts: _*).build

    val completeMultipartUploadRequest =
      CompleteMultipartUploadRequest.builder
        .bucket(bucket.value)
        .key(key.value)
        .uploadId(uploadId)
        .multipartUpload(multipartUpload)
        .build

    Concurrent[F]
      .delay(client.completeMultipartUpload(completeMultipartUploadRequest))
      .futureLift
      .guarantee(ContextShift[F].shift)
  }

  private def abortUpload(
    client: S3AsyncClient,
    uploadId: String,
    bucket: Bucket,
    key: ObjectKey): F[AbortMultipartUploadResponse] =
    Concurrent[F]
      .delay(
        client.abortMultipartUpload(
          AbortMultipartUploadRequest.builder
            .uploadId(uploadId)
            .bucket(bucket.value)
            .key(key.value)
            .build))
      .futureLift
      .guarantee(ContextShift[F].shift)
}
