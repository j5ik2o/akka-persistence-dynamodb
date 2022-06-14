/*
 * Copyright 2019 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.const

object DefaultColumnsDef {

  val PartitionKeyColumnName       = "pkey"
  val SortKeyColumnName            = "skey"
  val OrderingColumnName           = "ordering"
  val DeletedColumnName            = "deleted"
  val MessageColumnName            = "message"
  val PayloadColumnName            = "payload"
  val SerializerIdColumnName       = "serializer-id"
  val SerializerManifestColumnName = "serializer-manifest"
  val PersistenceIdColumnName      = "persistence-id"
  val SequenceNrColumnName         = "sequence-nr"
  val RevisionNrColumnName         = "revision"
  val TagsColumnName               = "tags"
  val SnapshotColumnName           = "snapshot"
  val CreatedColumnName            = "created"

}
