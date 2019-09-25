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
import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.contrib.pathy.AFile

import cats.Applicative
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, ExitCase}
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._
import eu.timepit.refined.auto._
import fs2.Stream
import monix.catnap.syntax._
import pathy.Path
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

class S3Destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr](
  client: S3AsyncClient,
  config: S3Config,
  partSize: Int) extends Destination[F] {
  import S3Destination.{ensureAbsFile, push}

  def destinationType: DestinationType = DestinationType("s3", 1L)

  def sinks: NonEmptyList[ResultSink[F]] =
    NonEmptyList(csvSink)

  private def csvSink = ResultSink.csv[F](RenderConfig.Csv()) {
    case (path, _, bytes) =>
      ensureAbsFile(path).flatMap(push(client, bytes, config, _, partSize))
  }
}

object S3Destination {
  def apply[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr](
    client: S3AsyncClient,
    config: S3Config,
    partSize: Int): S3Destination[F] =
    new S3Destination[F](client, config, partSize)

  private def ensureAbsFile[F[_]: Applicative: MonadResourceErr](r: ResourcePath): F[AFile] =
    r.fold(_.pure[F], MonadResourceErr[F].raiseError(ResourceError.notAResource(r)))

  private def nestResourcePath(file: AFile): AFile = {
    val withoutExtension = Path.fileName(Path.renameFile(file, _.dropExtension)).value
    val withExtension = Path.fileName(file)
    val parent = Path.fileParent(file)

    parent </> Path.dir(withoutExtension) </> Path.file1(withExtension)
  }

  private def push[F[_]: Concurrent](
    client: S3AsyncClient,
    bytes: Stream[F, Byte],
    config: S3Config,
    path: AFile,
    partSize: Int): F[Unit] = {
    val key = Path.posixCodec.printPath(nestResourcePath(path)).drop(1)

    Concurrent[F].bracketCase(
      startUpload(client, config.bucket, key))(createResponse =>
      for {
        parts <- uploadParts(client, bytes, createResponse.uploadId, partSize, config.bucket, key)
        _ <- completeUpload(client, createResponse.uploadId, config.bucket, key, parts)
      } yield ()) {
      case (createResponse, ExitCase.Canceled | ExitCase.Error(_)) =>
        abortUpload(client, createResponse.uploadId, config.bucket, key).void
      case (_, ExitCase.Completed) =>
        Concurrent[F].unit
    }
  }

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
        val partNumber = Int.box(n.toInt + 1)

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
