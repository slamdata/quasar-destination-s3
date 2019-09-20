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

import cats.effect.{ConcurrentEffect, ContextShift}
import cats.syntax.functor._
import eu.timepit.refined.auto._
import fs2.Stream
import monix.catnap.FutureLift.javaCompletableToConcurrent
import pathy.Path.posixCodec
import scalaz.NonEmptyList
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{
  CompleteMultipartUploadResponse,
  CompleteMultipartUploadRequest,
  CompletedMultipartUpload,
  CompletedPart,
  CreateMultipartUploadRequest,
  UploadPartRequest
}

class S3Destination[F[_]: ConcurrentEffect: ContextShift](
  client: S3AsyncClient,
  config: S3Config) extends Destination[F] {
  def destinationType: DestinationType = DestinationType("s3", 1L)

  def sinks: NonEmptyList[ResultSink[F]] =
    NonEmptyList(csvSink)

  // 10MB multipart uploads
  private val PartSize = 10 * 1024 * 1024

  private def csvSink = ResultSink.csv[F](true) {
    case (path, _, bytes) => {
      val F = ConcurrentEffect[F]

      val key = posixCodec.printPath(path.toPath).drop(1)

      def startUpload: F[String] = {
        val request =
          CreateMultipartUploadRequest.builder
            .bucket(config.bucket)
            .key(key)
            .build

        javaCompletableToConcurrent(F.delay(client.createMultipartUpload(request)))
          .map(_.uploadId)
      }

      def uploadParts(uploadId: String): Stream[F, CompletedPart] =
        bytes.chunkN(PartSize).zipWithIndex evalMap {
          case (byteChunk, n) => {
            // parts numbers must start at 1
            val partNumber = Int.box((n + 1).toInt)

            val uploadPartRequest =
              UploadPartRequest.builder
                .bucket(config.bucket)
                .uploadId(uploadId)
                .key(key)
                .partNumber(partNumber)
                .contentLength(Long.box(byteChunk.size.toLong))
                .build

            val uploadPartResponse =
              javaCompletableToConcurrent(F.delay(
                client.uploadPart(
                  uploadPartRequest,
                  AsyncRequestBody.fromByteBuffer(byteChunk.toByteBuffer))))

            val eTag = uploadPartResponse.map(_.eTag)

            val completedPart = eTag.map(et =>
              CompletedPart.builder.partNumber(partNumber).eTag(et).build)

            completedPart
          }
        }

      def streamToList[A](st: Stream[F, A]): Stream[F, List[A]] =
        st.fold(List.empty[A]) {
          case (acc, part) => acc :+ part
        }

      def completeUpload(uploadId: String, parts: List[CompletedPart])
          : F[CompleteMultipartUploadResponse] = {
        val multipartUpload =
          CompletedMultipartUpload.builder.parts(parts :_*).build

        val completeMultipartUploadRequest =
          CompleteMultipartUploadRequest.builder
            .bucket(config.bucket)
            .key(key)
            .uploadId(uploadId)
            .multipartUpload(multipartUpload)
            .build

        javaCompletableToConcurrent(
          F.delay(client.completeMultipartUpload(completeMultipartUploadRequest)))
      }

      (for {
        uid <- Stream.eval(startUpload)
        parts <- streamToList(uploadParts(uid))
        _ <- Stream.eval(completeUpload(uid, parts))
      } yield ()).compile.drain
    }
  }
}

object S3Destination {
  def apply[F[_]: ConcurrentEffect: ContextShift](client: S3AsyncClient, config: S3Config): S3Destination[F] =
    new S3Destination[F](client, config)
}
