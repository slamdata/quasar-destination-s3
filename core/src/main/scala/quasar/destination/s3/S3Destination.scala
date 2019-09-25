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

import cats.effect.Concurrent
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._
import eu.timepit.refined.auto._
import pathy.Path
import scalaz.NonEmptyList

class S3Destination[F[_]: Concurrent: MonadResourceErr](bucket: Bucket, upload: Upload[F])
    extends Destination[F] {
  def destinationType: DestinationType = DestinationType("s3", 1L)

  def sinks: NonEmptyList[ResultSink[F]] =
    NonEmptyList(csvSink)

  private def csvSink = ResultSink.csv[F](RenderConfig.Csv()) {
    case (path, _, bytes) =>
      for {
        afile <- ensureAbsFile(path)
        key = Path.posixCodec.printPath(nestResourcePath(afile)).drop(1)
        _ <- upload.push(bytes, bucket, key)
      } yield ()
  }

  private def nestResourcePath(file: AFile): AFile = {
    val withoutExtension = Path.fileName(Path.renameFile(file, _.dropExtension)).value
    val withExtension = Path.fileName(file)
    val parent = Path.fileParent(file)

    parent </> Path.dir(withoutExtension) </> Path.file1(withExtension)
  }

  private def ensureAbsFile(r: ResourcePath): F[AFile] =
    r.fold(_.pure[F], MonadResourceErr[F].raiseError(ResourceError.notAResource(r)))
}

object S3Destination {
  def apply[F[_]: Concurrent: MonadResourceErr](bucket: Bucket, upload: Upload[F])
      : S3Destination[F] =
    new S3Destination[F](bucket, upload)
}
