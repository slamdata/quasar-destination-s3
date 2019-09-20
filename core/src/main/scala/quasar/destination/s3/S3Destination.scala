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

import slamdata.Predef.{Stream => _, _}

import quasar.api.destination.{Destination, DestinationType, ResultSink}
import quasar.api.push.RenderConfig

import cats.effect.{Concurrent, ConcurrentEffect, ContextShift}
import cats.syntax.functor._
import cats.syntax.flatMap._
import eu.timepit.refined.auto._
import fs2.Stream
import monix.catnap.syntax._
import pathy.Path.posixCodec
import scalaz.NonEmptyList
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

class S3Destination[F[_]: ConcurrentEffect: ContextShift](
  client: S3AsyncClient,
  config: S3Config) extends Destination[F] {
  def destinationType: DestinationType = DestinationType("s3", 1L)

  def sinks: NonEmptyList[ResultSink[F]] =
    NonEmptyList(csvSink)

  // Minimum 10MiB multipart uploads
  private val PartSize = 10 * 1024 * 1024

  private def csvSink = ResultSink.csv[F](RenderConfig.Csv()) {
    case (path, _, bytes) => {
      val key = posixCodec.printPath(path.toPath).drop(1)

      for {
        startUploadResponse <- S3Destination.startUpload(client, config.bucket, key)
        uid = startUploadResponse.uploadId
        parts <- S3Destination.uploadParts(client, bytes, uid, PartSize, config.bucket, key)
        completeUploadResponse <- S3Destination.completeUpload(client, uid, config.bucket, key, parts)
      } yield ()
    }
  }
}

object S3Destination {
  def apply[F[_]: ConcurrentEffect: ContextShift](client: S3AsyncClient, config: S3Config)
      : S3Destination[F] =
    new S3Destination[F](client, config)

  private def startUpload[F[_]: Concurrent](client: S3AsyncClient, bucket: String, key: String)
      : F[CreateMultipartUploadResponse] =
    Concurrent[F].delay(client.createMultipartUpload(
      CreateMultipartUploadRequest.builder.bucket(bucket).key(key).build)).futureLift

  private def uploadParts[F[_]: Concurrent](
    client: S3AsyncClient,
    bytes: Stream[F, Byte],
    uploadId: String,
    minChunkSize: Int,
    bucket: String,
    key: String): F[List[CompletedPart]] =
    (bytes.chunkMin(minChunkSize).zipWithIndex evalMap {
      case (byteChunk, n) => {
        // parts numbers must start at 1
        val partNumber = Int.box((n + 1).toInt)

        val uploadPartRequest =
          UploadPartRequest.builder
            .bucket(bucket)
            .uploadId(uploadId)
            .key(key)
            .partNumber(partNumber)
            .contentLength(Long.box(byteChunk.size.toLong))
            .build

        val uploadPartResponse =
          Concurrent[F].delay(
            client.uploadPart(
              uploadPartRequest,
              AsyncRequestBody.fromByteBuffer(byteChunk.toByteBuffer))).futureLift

        uploadPartResponse.map(response =>
          CompletedPart.builder.partNumber(partNumber).eTag(response.eTag).build)
      }
    }).compile.toList

  private def completeUpload[F[_]: Concurrent](
    client: S3AsyncClient,
    uploadId: String,
    bucket: String,
    key: String,
    parts: List[CompletedPart]): F[CompleteMultipartUploadResponse] = {
    val multipartUpload =
      CompletedMultipartUpload.builder.parts(parts :_*).build

    val completeMultipartUploadRequest =
      CompleteMultipartUploadRequest.builder
        .bucket(bucket)
        .key(key)
        .uploadId(uploadId)
        .multipartUpload(multipartUpload)
        .build

    Concurrent[F].delay(
      client.completeMultipartUpload(completeMultipartUploadRequest)).futureLift
  }

  private def abortUpload[F[_]: Concurrent](
    client: S3AsyncClient,
    uploadId: String,
    bucket: String,
    key: String): F[AbortMultipartUploadResponse] =
    Concurrent[F].delay(
      client.abortMultipartUpload(
        AbortMultipartUploadRequest
          .builder
          .uploadId(uploadId)
          .bucket(bucket)
          .key(key)
          .build)).futureLift
}
