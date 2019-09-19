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
import cats.syntax.flatMap._
import cats.syntax.functor._
import eu.timepit.refined.auto._
import fs2.Stream
import monix.catnap.FutureLift.javaCompletableToConcurrent
import pathy.Path.posixCodec
import scalaz.NonEmptyList
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{
  CompletedPart,
  CompletedMultipartUpload,
  CompleteMultipartUploadRequest,
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

      val key = posixCodec.printPath(path.toPath)

      val request =
        CreateMultipartUploadRequest.builder
          .bucket(config.bucket)
          .key(key)
          .build

      // WE ARE CREATING MORE THAN ONE UPLOAD. THAT's WHY IT DOESNT WORK
      // ONE TO UPLOAD AND ANOTHER ONE TO CLOSE THE UPLOAD
      val uploadId: F[String] =
        javaCompletableToConcurrent(F.delay(client.createMultipartUpload(request)))
          .map(_.uploadId)

      val uploadParts: Stream[F, CompletedPart] =
        Stream.eval(uploadId).flatMap(uid =>
          bytes.chunkN(PartSize).zipWithIndex.evalMap {
            case (byteChunk, n) => {
              // parts numbers start at 1
              val partNumber = Int.box((n + 1).toInt)
              println(s"Upload Id: $uid")
              println(s"Processing part $partNumber")

              val uploadPartRequest =
                UploadPartRequest.builder
                  .bucket(config.bucket)
                  .uploadId(uid)
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
          })

      val uploadPartsList = uploadParts.fold(List.empty[CompletedPart]) {
        case (acc, part) => acc :+ part
      }

      val completedMultipartUpload =
        uploadPartsList.flatMap(parts => for {
          _ <- Stream.eval(F.delay(println(s"Parts: $parts")))
          cmu = CompletedMultipartUpload.builder.parts(parts :_*).build
        } yield cmu)

      val complete = completedMultipartUpload evalMap { multipartUpload =>
        val completeMultipartUploadRequest =
          uploadId.map(uid =>
            CompleteMultipartUploadRequest.builder
              .bucket(config.bucket)
              .key(key)
              .uploadId(uid)
              .multipartUpload(multipartUpload)
              .build)

        javaCompletableToConcurrent(
          completeMultipartUploadRequest.flatMap(
            request => F.delay(client.completeMultipartUpload(request))))
      }

      complete.compile.drain
    }
  }
}

object S3Destination {
  def apply[F[_]: ConcurrentEffect: ContextShift](client: S3AsyncClient, config: S3Config): S3Destination[F] =
    new S3Destination[F](client, config)
}
