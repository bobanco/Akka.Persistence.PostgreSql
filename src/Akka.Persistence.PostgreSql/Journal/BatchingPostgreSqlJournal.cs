using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Sql.Common.Journal;
using Akka.Serialization;
using Akka.Util;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;

namespace Akka.Persistence.PostgreSql.Journal
{
    public sealed class BatchingPostgresJournalSetup : BatchingSqlJournalSetup
    {
        public readonly StoredAsType StoredAs;
        public readonly JsonSerializerSettings JsonSerializerSettings;

        public BatchingPostgresJournalSetup(Config config) : base(config, new QueryConfiguration(
            schemaName: config.GetString("schema-name", "public"),
            journalEventsTableName: config.GetString("table-name", "event_journal"),
            metaTableName: config.GetString("metadata-table-name", "metadata"),
            persistenceIdColumnName: "persistence_id",
            sequenceNrColumnName: "sequence_nr",
            payloadColumnName: "payload",
            manifestColumnName: "manifest",
            timestampColumnName: "created_at",
            isDeletedColumnName: "is_deleted",
            tagsColumnName: "tags",
            orderingColumnName: "ordering",
            timeout: config.GetTimeSpan("connection-timeout", TimeSpan.FromSeconds(30)),
            serializerIdColumnName: "serializer_id",
            defaultSerializer: "serializer"))
        {
            var storedAsString = config.GetString("stored-as");
            StoredAsType storedAs;
            if (!Enum.TryParse(storedAsString, true, out storedAs))
            {
                throw new ConfigurationException($"Value [{storedAsString}] of the 'stored-as' HOCON config key is not valid. Valid values: bytea, json, jsonb.");
            }
            StoredAs = storedAs;
            JsonSerializerSettings = new JsonSerializerSettings
            {
                ContractResolver = new AkkaContractResolver()
            };

        }

        public BatchingPostgresJournalSetup(string connectionString, int maxConcurrentOperations, int maxBatchSize, int maxBufferSize, bool autoInitialize, TimeSpan connectionTimeout, IsolationLevel isolationLevel, CircuitBreakerSettings circuitBreakerSettings, ReplayFilterSettings replayFilterSettings, QueryConfiguration namingConventions, string defaultSerializer, StoredAsType storedAs, JsonSerializerSettings jsonSerializerSettings = null) 
            : base(connectionString, maxConcurrentOperations, maxBatchSize, maxBufferSize, autoInitialize, connectionTimeout, isolationLevel, circuitBreakerSettings, replayFilterSettings, namingConventions, defaultSerializer)
        {
            StoredAs = storedAs;
            JsonSerializerSettings = jsonSerializerSettings?? new JsonSerializerSettings
            {
                ContractResolver = new AkkaContractResolver()
            };
        }
    }
    public class BatchingPostgreSqlJournal : BatchingSqlJournal<NpgsqlConnection, NpgsqlCommand>
    {
        private readonly Func<IPersistentRepresentation, SerializationResult> _serialize;
        private readonly Func<Type, object, string, int?, object> _deserialize;
        private readonly ITimestampProvider _timestampProvider;
        public BatchingPostgreSqlJournal(Config config)
            : this(new BatchingPostgresJournalSetup(config))
        {
            _timestampProvider = GetTimestampProvider(config.GetString("timestamp-provider"));
        }

        public BatchingPostgreSqlJournal(BatchingPostgresJournalSetup setup) : base(setup)
        {
            var conventions = Setup.NamingConventions;
            ByTagSql = $@"
             SELECT TOP (@Take)
             e.{conventions.PersistenceIdColumnName} as PersistenceId, 
             e.{conventions.SequenceNrColumnName} as SequenceNr, 
             e.{conventions.TimestampColumnName} as Timestamp, 
             e.{conventions.IsDeletedColumnName} as IsDeleted, 
             e.{conventions.ManifestColumnName} as Manifest, 
             e.{conventions.PayloadColumnName} as Payload,
             e.{conventions.SerializerIdColumnName} as SerializerId,
             e.{conventions.OrderingColumnName} as Ordering
             FROM {conventions.FullJournalTableName} e
             WHERE e.{conventions.OrderingColumnName} > @Ordering AND e.{conventions.TagsColumnName} LIKE @Tag
             ORDER BY {conventions.OrderingColumnName} ASC
             ";
            Initializers = ImmutableDictionary.CreateRange(new[]
            {
                new KeyValuePair<string, string>("CreateJournalSql", $@"
                CREATE TABLE IF NOT EXISTS {conventions.FullJournalTableName} (
                    {conventions.OrderingColumnName} BIGSERIAL NOT NULL PRIMARY KEY,
                    {conventions.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                    {conventions.SequenceNrColumnName} BIGINT NOT NULL,
                    {conventions.IsDeletedColumnName} BOOLEAN NOT NULL,
                    {conventions.TimestampColumnName} BIGINT NOT NULL,
                    {conventions.ManifestColumnName} VARCHAR(500) NOT NULL,
                    {conventions.PayloadColumnName} {setup.StoredAs} NOT NULL,
                    {conventions.TagsColumnName} VARCHAR(100) NULL,
                    {conventions.SerializerIdColumnName} BIGINT,                   
                    CONSTRAINT {conventions.JournalEventsTableName}_uq UNIQUE ({conventions.PersistenceIdColumnName}, {conventions.SequenceNrColumnName})
                );"),
                new KeyValuePair<string, string>("CreateMetadataSql", $@"
                CREATE TABLE IF NOT EXISTS {conventions.FullMetaTableName} (
                    {conventions.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                    {conventions.SequenceNrColumnName} BIGINT NOT NULL,
                    CONSTRAINT {conventions.MetaTableName}_pk PRIMARY KEY ({conventions.PersistenceIdColumnName}, {conventions.SequenceNrColumnName})
                );"),
            });

            switch (setup.StoredAs)
            {
                case StoredAsType.ByteA:
                    _serialize = e =>
                    {
                        var serializer = Context.System.Serialization.FindSerializerFor(e.Payload);
                        return new SerializationResult(NpgsqlDbType.Bytea, serializer.ToBinary(e.Payload), serializer);
                    };
                    _deserialize = (type, serialized, manifest, serializerId) =>
                    {
                        if (serializerId.HasValue)
                        {
                            return Context.System.Serialization.Deserialize((byte[])serialized, serializerId.Value, manifest);
                        }
                        else
                        {
                            // Support old writes that did not set the serializer id
                            var deserializer = Context.System.Serialization.FindSerializerForType(type, setup.DefaultSerializer);
                            return deserializer.FromBinary((byte[])serialized, type);
                        }
                    };
                    break;
                case StoredAsType.JsonB:
                    _serialize = e => new SerializationResult(NpgsqlDbType.Jsonb, JsonConvert.SerializeObject(e.Payload, setup.JsonSerializerSettings), null);
                    _deserialize = (type, serialized, manifest, serializerId) => JsonConvert.DeserializeObject((string)serialized, type, setup.JsonSerializerSettings);
                    break;
                case StoredAsType.Json:
                    _serialize = e => new SerializationResult(NpgsqlDbType.Json, JsonConvert.SerializeObject(e.Payload, setup.JsonSerializerSettings), null);
                    _deserialize = (type, serialized, manifest, serializerId) => JsonConvert.DeserializeObject((string)serialized, type, setup.JsonSerializerSettings);
                    break;
                default:
                    throw new NotSupportedException($"{setup.StoredAs} is not supported Db type for a payload");
            }
        }

        protected override NpgsqlConnection CreateConnection(string connectionString) => new NpgsqlConnection(connectionString);

        protected override ImmutableDictionary<string, string> Initializers { get; }

        protected override string ByTagSql { get; }

        protected override void WriteEvent(NpgsqlCommand command, IPersistentRepresentation e, string tags = "")
        {
            var serializationResult = _serialize(e);
            var serializer = serializationResult.Serializer;
            var hasSerializer = serializer != null;

            string manifest = "";
            if (hasSerializer && serializer is SerializerWithStringManifest)
                manifest = ((SerializerWithStringManifest)serializer).Manifest(e.Payload);
            else if (hasSerializer && serializer.IncludeManifest)
                manifest = QualifiedName(e);
            else
                manifest = string.IsNullOrEmpty(e.Manifest) ? QualifiedName(e) : e.Manifest;

            AddParameter(command, "@PersistenceId", DbType.String, e.PersistenceId);
            AddParameter(command, "@SequenceNr", DbType.Int64, e.SequenceNr);
            AddParameter(command, "@Timestamp", DbType.Int64, _timestampProvider.GenerateTimestamp(e));
            AddParameter(command, "@IsDeleted", DbType.Boolean, false);
            AddParameter(command, "@Manifest", DbType.String, manifest);

            if (hasSerializer)
            {
                AddParameter(command, "@SerializerId", DbType.Int32, serializer.Identifier);
            }
            else
            {
                AddParameter(command, "@SerializerId", DbType.Int32, DBNull.Value);
            }

            command.Parameters.Add(new NpgsqlParameter("@Payload", serializationResult.DbType) { Value = serializationResult.Payload });

            AddParameter(command, "@Tag", DbType.String, tags);
        }

        private static string QualifiedName(IPersistentRepresentation e)
        {
            var type = e.Payload.GetType();
            return type.TypeQualifiedName();
        }

        protected override IPersistentRepresentation ReadEvent(DbDataReader reader)
        {
            var persistenceId = reader.GetString(PersistenceIdIndex);
            var sequenceNr = reader.GetInt64(SequenceNrIndex);
            //var timestamp = reader.GetDateTime(TimestampIndex);
            var isDeleted = reader.GetBoolean(IsDeletedIndex);
            var manifest = reader.GetString(ManifestIndex);
            var raw = reader[PayloadIndex];

            int? serializerId = null;
            Type type = null;
            if (reader.IsDBNull(SerializerIdIndex))
            {
                type = Type.GetType(manifest, true);
            }
            else
            {
                serializerId = reader.GetInt32(SerializerIdIndex);
            }

            var deserialized = _deserialize(type, raw, manifest, serializerId);

            return new Persistent(deserialized, sequenceNr, persistenceId, manifest, isDeleted, ActorRefs.NoSender, null);
        }

        protected ITimestampProvider GetTimestampProvider(string typeName)
        {
            var type = Type.GetType(typeName, true);
            try
            {
                return (ITimestampProvider)Activator.CreateInstance(type, Context.System);
            }
            catch (Exception)
            {
                return (ITimestampProvider)Activator.CreateInstance(type);
            }
        }
    }
}
