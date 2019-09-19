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

import quasar.api.destination.DestinationError
import quasar.api.destination.DestinationError.InitializationError
import quasar.api.destination.{Destination, DestinationType}
import quasar.connector.{DestinationModule, MonadResourceErr}

import scala.util.Either

import argonaut.Json
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.regions.{Region => AwsRegion}
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.syntax.either._
import eu.timepit.refined.auto._
import scalaz.NonEmptyList

object S3DestinationModule extends DestinationModule {
  def destinationType = DestinationType("s3", 1L)

  def sanitizeDestinationConfig(config: Json) = config

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json)
      : Resource[F, Either[InitializationError[Json], Destination[F]]] = {

    val decodedConfig = config.as[S3Config].toEither.leftMap {
      case (err, _) => DestinationError.invalidConfiguration((destinationType, config, NonEmptyList(err)))
    }

    decodedConfig.fold(
      err => Resource.pure(err.asLeft),
      config => mkClient(config).map(client => S3Destination[F](client, config).asRight))
  }

  private def mkClient[F[_]: Sync](cfg: S3Config): Resource[F, S3AsyncClient] = {
    val client =
      Sync[F].delay(
        S3AsyncClient
          .builder
          .credentialsProvider(
            StaticCredentialsProvider.create(
              AwsBasicCredentials.create(
                cfg.credentials.accessKey.value,
                cfg.credentials.secretKey.value)))
          .region(AwsRegion.of(cfg.credentials.region.value))
          .build)

    Resource.fromAutoCloseable[F, S3AsyncClient](client)
  }
}
