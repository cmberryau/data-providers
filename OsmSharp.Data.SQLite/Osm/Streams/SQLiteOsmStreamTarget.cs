// OsmSharp - OpenStreetMap (OSM) SDK
// Copyright (C) 2015 Abelshausen Ben
// 
// This file is part of OsmSharp.
// 
// OsmSharp is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
// 
// OsmSharp is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with OsmSharp. If not, see <http://www.gnu.org/licenses/>.

using OsmSharp.Collections.Tags;
using OsmSharp.Osm;
using OsmSharp.Osm.Streams;
using OsmSharp.Osm.Tiles;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Text;
using OsmSharp.Osm.Filters;

namespace OsmSharp.Data.SQLite.Osm.Streams
{
    /// <summary>
    /// Data target for SQLite.
    /// </summary>
    public class SQLiteOsmStreamTarget : OsmStreamTarget
    {
        /// <summary>
        /// Does this object own it's own connection?
        /// </summary>
        public bool ConnectionOwner;

        /// <summary>
        /// Indicates if the SQLite target supports concurrent copies
        /// </summary>
        public override bool SupportsConcurrentCopies
        {
            get
            {
                return false;
            }
        }

        private SQLiteConnection _connection;
        private readonly string _connection_string;
        private readonly bool _create_and_detect_schema;

        private const string Replace = @"REPLACE INTO {0} " +
                                         @"( " +
                                           @"{1} " +
                                         @") " +
                                       @"VALUES " +
                                           @"{2};";

        private Dictionary<OsmGeoType, SQLiteCommand> _replaceTagBatchCommands;
        private Dictionary<OsmGeoType, SQLiteCommand> _replaceTagFlushCommands;
        private SQLiteCommand _replaceNodeBatchCommand;
        private SQLiteCommand _replaceNodeFlushCommand;
        private SQLiteCommand _replaceWayBatchCommand;
        private SQLiteCommand _replaceWayFlushCommand;
        private SQLiteCommand _replaceWayNodesBatchCommand;
        private SQLiteCommand _replaceWayNodesFlushCommand;
        private SQLiteCommand _replaceRelationBatchCommand;
        private SQLiteCommand _replaceRelationFlushCommand;
        private SQLiteCommand _replaceRelationMembersBatchCommand;
        private SQLiteCommand _replaceRelationMembersFlushCommand;

        private const int DefaultNodeBatchCount = 128;
        private const int NodeParamsCount = 6;
        private const int DefaultWayBatchCount = 128;
        private const int WayParamsCount = 3;
        private const int DefaultWayNodesBatchCount = 256;
        private const int WayNodesParamsCount = 3;
        private const int DefaultRelationBatchCount = 128;
        private const int RelationParamsCount = 3;
        private const int DefaultRelationMembersBatchCount = 128;
        private const int RelationMemembersParamsCount = 3;
        private const int DefaultTagBatchCount = 256;

        private const int TagsBatchCount = DefaultTagBatchCount;
        private List<Tuple<int, Tag>> _cached_node_tags;
        private List<Tuple<int, Tag>> _cached_way_tags;
        private List<Tuple<int, Tag>> _cached_relation_tags;
        private long _total_node_tags_count;
        private long _total_way_tags_count;
        private long _total_relation_tags_count;

        private const int NodeBatchCount = DefaultNodeBatchCount;
        private List<Node> _cached_nodes;
        private long _node_count;

        private const int WayBatchCount = DefaultWayBatchCount;
        private List<Way> _cached_ways;
        private long _total_way_count;

        private const int WayNodesBatchCount = DefaultWayNodesBatchCount;
        private List<Tuple<long?, long?, long?>> _cached_way_nodes;
        private long _total_way_nodes_count;

        private const int RelationBatchCount = DefaultRelationBatchCount;
        private List<Relation> _cached_relations;
        private long _total_relation_count;

        private const int RelationMembersBatchCount = DefaultRelationMembersBatchCount;
        private List<Tuple<long?, RelationMember, long?>> _cached_relation_members;
        private long _total_relation_members_count;

        private readonly Filter _geo_filter;
        private readonly TagFilter _tag_filter;

        private Dictionary<TagsCollectionBase, int> _unique_node_tags;
        private Dictionary<TagsCollectionBase, int> _unique_way_tags;
        private Dictionary<TagsCollectionBase, int> _unique_relation_tags;

        /// <summary>
        /// Creates a new SQLite target
        /// </summary>
        /// <param name="connection">The SQLite connection to use</param>
        /// <param name="create_schema">Do the db tables need to be created?</param>
        /// <param name="geo_filter">The geos filter to be used</param>
        /// <param name="tag_filter">The tags filter to be used</param>
        public SQLiteOsmStreamTarget(SQLiteConnection connection, bool create_schema = false,
                                     Filter geo_filter = null, TagFilter tag_filter = null)
        {
            if (geo_filter == null)
            {
                geo_filter = Filter.Any();
            }

            _geo_filter = geo_filter;

            if (tag_filter == null)
            {
                tag_filter = TagFilter.Any();
            }

            _tag_filter = tag_filter;

            _connection = connection;
            _connection_string = connection.ConnectionString;

            _create_and_detect_schema = create_schema;
            ConnectionOwner = false;
        }

        /// <summary>
        /// Creates a new SQLite target
        /// </summary>
        /// <param name="in_memory">Is the DB in memory? (default is false)</param>
        /// <param name="path">The path to the DB, or its descriptor in memory (if any)</param>
        /// <param name="password">The DB password (if any)</param>
        /// <param name="create_schema">Do the db tables need to be created?</param>
        /// <param name="geo_filter">The geos filter to be used</param>
        /// <param name="tag_filter">The tags filter to be used</param>
        public SQLiteOsmStreamTarget(bool in_memory = false, string path = null,
                                     string password = null, bool create_schema = false,
                                     Filter geo_filter = null, TagFilter tag_filter = null)
        {
            if (geo_filter == null)
            {
                geo_filter = Filter.Any();
            }

            _geo_filter = geo_filter;

            if (tag_filter == null)
            {
                tag_filter = TagFilter.Any();
            }

            _tag_filter = tag_filter;

            _connection_string = SQLiteSchemaTools.BuildConnectionString(in_memory, path, password);

            _create_and_detect_schema = create_schema;
            ConnectionOwner = false;
        }

        /// <summary>
        /// Creates a new SQLite target
        /// </summary>
        /// <param name="connection_string">The SQLite connection string to use</param>
        /// <param name="create_schema">Should we detect if the db tables exist and attempt to create them?</param>
        /// <param name="geo_filter">The geos filter to be used</param>
        /// <param name="tag_filter">The tags filter to be used</param>
        public SQLiteOsmStreamTarget(string connection_string, bool create_schema = false,
                                     Filter geo_filter = null, TagFilter tag_filter = null)
        {
            if (geo_filter == null)
            {
                geo_filter = Filter.Any();
            }

            _geo_filter = geo_filter;

            if (tag_filter == null)
            {
                tag_filter = TagFilter.Any();
            }

            _tag_filter = tag_filter;

            _connection_string = connection_string;

            _create_and_detect_schema = create_schema;
            ConnectionOwner = false;
        }

        /// <summary>
        /// Initializes this target.
        /// </summary>
        public override void Initialize()
        {
            if (_connection == null)
            {
                _connection = new SQLiteConnection(_connection_string);
                ConnectionOwner = true;
            }
            if (_connection.State != System.Data.ConnectionState.Open)
            {
                _connection.Open();
            }

            InitializeCommands(_connection);
            InitializeCache();

            if (_create_and_detect_schema)
            { // creates or detects the tables
                SQLiteSchemaTools.CreateAndDetect(_connection);
            }
        }


        /// <summary>
        /// Adds a node.
        /// </summary>
        /// <param name="node">The node to add</param>
        public override void AddNode(Node node)
        {
            if (_cached_nodes.Count == NodeBatchCount)
            {
                BatchAddNodes(_cached_nodes, _replaceNodeBatchCommand);
            }

            _cached_nodes.Add(node);

            AddTags(node, _unique_node_tags, _cached_node_tags);
        }

        /// <summary>
        /// Adds a way.
        /// </summary>
        /// <param name="way">The way to add to the db</param>
        public override void AddWay(Way way)
        {
            if (_geo_filter.Evaluate(way))
            {
                if (_cached_ways.Count == WayBatchCount)
                {
                    BatchAddWays(_cached_ways, _replaceWayBatchCommand);
                }

                _cached_ways.Add(way);

                AddTags(way, _unique_way_tags, _cached_way_tags);
                AddWayNodes(way);
            }
        }

        /// <summary>
        /// Adds a relation.
        /// </summary>
        /// <param name="relation">The relation to add to the SQLite DB</param>
        public override void AddRelation(Relation relation)
        {
            if (_geo_filter.Evaluate(relation))
            {
                if (_cached_relations.Count == RelationBatchCount)
                {
                    BatchAddRelations(_cached_relations, _replaceRelationBatchCommand);
                }

                _cached_relations.Add(relation);

                AddTags(relation, _unique_relation_tags, _cached_relation_tags);
                AddRelationMembers(relation);
            }
        }

        /// <summary>
        /// Closes this target.
        /// </summary>
        public override void Close()
        {
            DisposeCommandDictionary(_replaceTagBatchCommands);
            DisposeCommandDictionary(_replaceTagFlushCommands);
            _replaceNodeBatchCommand.Dispose();
            _replaceNodeFlushCommand.Dispose();
            _replaceWayBatchCommand.Dispose();
            _replaceWayFlushCommand.Dispose();
            _replaceWayNodesBatchCommand.Dispose();
            _replaceWayNodesFlushCommand.Dispose();
            _replaceRelationBatchCommand.Dispose();
            _replaceRelationFlushCommand.Dispose();
            _replaceRelationMembersBatchCommand.Dispose();
            _replaceRelationMembersFlushCommand.Dispose();

            if (ConnectionOwner)
            {
                if (_connection != null && !Utilities.IsNullOrWhiteSpace(_connection_string))
                {
                    _connection.Close();
                    _connection.Dispose();

                    _connection = null;
                }
            }
        }

        /// <summary>
        /// Called right after pull and right before flush.
        /// </summary>
	    public override void OnAfterPull()
        {
            base.OnAfterPull();

            // flush all pending data
            Flush();

            // filter the database
            SQLiteSchemaTools.PostFilter(_connection);
        }

        /// <summary>
        /// Flushes all data.
        /// </summary>
        public override void Flush()
        {
            if (_cached_nodes.Count > 0)
            {
                FlushAddNodes(_cached_nodes, _replaceNodeFlushCommand);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.SQLite.Osm.Streams.SQLiteOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _node_count, "node");
            }

            if (_cached_node_tags.Count > 0)
            {
                FlushAddTags(_cached_node_tags, OsmGeoType.Node);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.SQLite.Osm.Streams.SQLiteOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_node_tags_count, "node_tags");
            }

            if (_cached_ways.Count > 0)
            {
                FlushAddWays(_cached_ways, _replaceWayFlushCommand);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.SQLite.Osm.Streams.SQLiteOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_way_count, "way");
            }

            if (_cached_way_tags.Count > 0)
            {
                FlushAddTags(_cached_way_tags, OsmGeoType.Way);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.SQLite.Osm.Streams.SQLiteOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_way_tags_count, "way_tags");
            }

            if (_cached_way_nodes.Count > 0)
            {
                FlushAddWayNodes(_cached_way_nodes, _replaceWayNodesFlushCommand);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.SQLite.Osm.Streams.SQLiteOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_way_nodes_count, "way_nodes");
            }

            if (_cached_relations.Count > 0)
            {
                FlushAddRelations(_cached_relations, _replaceRelationFlushCommand);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.SQLite.Osm.Streams.SQLiteOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_relation_count, "relation");
            }

            if (_cached_relation_tags.Count > 0)
            {
                FlushAddTags(_cached_relation_tags, OsmGeoType.Relation);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.SQLite.Osm.Streams.SQLiteOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_relation_tags_count, "relation_tags");
            }

            if (_cached_relation_members.Count > 0)
            {
                FlushAddRelationMembers(_cached_relation_members, _replaceRelationMembersFlushCommand);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.SQLite.Osm.Streams.SQLiteOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_relation_members_count, "relation_members");
            }
        }

        private void InitializeCommands(SQLiteConnection connection)
        {
            // tag commands
            _replaceTagBatchCommands = new Dictionary<OsmGeoType, SQLiteCommand>();
            _replaceTagFlushCommands = new Dictionary<OsmGeoType, SQLiteCommand>();

            var command_params = new Tuple<string, DbType>[3];

            command_params[0] = new Tuple<string, DbType>("id", DbType.Int32);
            command_params[1] = new Tuple<string, DbType>("tag_key", DbType.String);
            command_params[2] = new Tuple<string, DbType>("value", DbType.String);

            _replaceTagBatchCommands[OsmGeoType.Node] = CreateBatchCommand(connection, Replace, "node_tags",
                DefaultTagBatchCount, command_params);
            _replaceTagFlushCommands[OsmGeoType.Node] = CreateFlushCommand(connection, Replace, "node_tags",
                command_params);

            _replaceTagBatchCommands[OsmGeoType.Way] = CreateBatchCommand(connection, Replace, "way_tags",
                DefaultTagBatchCount, command_params);
            _replaceTagFlushCommands[OsmGeoType.Way] = CreateFlushCommand(connection, Replace, "way_tags",
                command_params);

            _replaceTagBatchCommands[OsmGeoType.Relation] = CreateBatchCommand(connection, Replace, "relation_tags",
                DefaultTagBatchCount, command_params);
            _replaceTagFlushCommands[OsmGeoType.Relation] = CreateFlushCommand(connection, Replace, "relation_tags",
                command_params);

            // node commands
            command_params = new Tuple<string, DbType>[6];

            command_params[0] = new Tuple<string, DbType>("id", DbType.Int64);
            command_params[1] = new Tuple<string, DbType>("latitude", DbType.Int32);
            command_params[2] = new Tuple<string, DbType>("longitude", DbType.Int32);
            command_params[3] = new Tuple<string, DbType>("tile", DbType.Int64);
            command_params[4] = new Tuple<string, DbType>("tags_id", DbType.Int32);
            command_params[5] = new Tuple<string, DbType>("name", DbType.String);

            _replaceNodeBatchCommand = CreateBatchCommand(connection, Replace, "node",
                NodeBatchCount, command_params);
            _replaceNodeFlushCommand = CreateFlushCommand(connection, Replace, "node",
                command_params);

            // way commands
            command_params = new Tuple<string, DbType>[3];

            command_params[0] = new Tuple<string, DbType>("id", DbType.Int64);
            command_params[1] = new Tuple<string, DbType>("tags_id", DbType.Int32);
            command_params[2] = new Tuple<string, DbType>("name", DbType.String);

            _replaceWayBatchCommand = CreateBatchCommand(connection, Replace, "way",
                WayBatchCount, command_params);
            _replaceWayFlushCommand = CreateFlushCommand(connection, Replace, "way",
                command_params);

            command_params = new Tuple<string, DbType>[3];

            command_params[0] = new Tuple<string, DbType>("way_id", DbType.Int64);
            command_params[1] = new Tuple<string, DbType>("node_id", DbType.Int64);
            command_params[2] = new Tuple<string, DbType>("sequence_id", DbType.Int64);

            _replaceWayNodesBatchCommand = CreateBatchCommand(connection, Replace, "way_nodes",
                WayNodesBatchCount, command_params);
            _replaceWayNodesFlushCommand = CreateFlushCommand(connection, Replace, "way_nodes",
                command_params);

            command_params = new Tuple<string, DbType>[3];

            command_params[0] = new Tuple<string, DbType>("id", DbType.Int64);
            command_params[1] = new Tuple<string, DbType>("tags_id", DbType.Int64);
            command_params[2] = new Tuple<string, DbType>("name", DbType.String);

            // relation commands
            _replaceRelationBatchCommand = CreateBatchCommand(connection, Replace, "relation",
                RelationBatchCount, command_params);
            _replaceRelationFlushCommand = CreateFlushCommand(connection, Replace, "relation",
                command_params);

            command_params = new Tuple<string, DbType>[5];

            command_params[0] = new Tuple<string, DbType>("relation_id", DbType.Int64);
            command_params[1] = new Tuple<string, DbType>("member_type", DbType.Byte);
            command_params[2] = new Tuple<string, DbType>("member_id", DbType.Int64);
            command_params[3] = new Tuple<string, DbType>("member_role", DbType.String);
            command_params[4] = new Tuple<string, DbType>("sequence_id", DbType.Int64);

            _replaceRelationMembersBatchCommand = CreateBatchCommand(connection, Replace, "relation_members",
                RelationMembersBatchCount, command_params);
            _replaceRelationMembersFlushCommand = CreateFlushCommand(connection, Replace, "relation_members",
                command_params);
        }

        private void InitializeCache()
        {
            // tag caches
            _cached_node_tags = new List<Tuple<int, Tag>>();
            _unique_node_tags = new Dictionary<TagsCollectionBase, int>();
            _cached_way_tags = new List<Tuple<int, Tag>>();
            _unique_way_tags = new Dictionary<TagsCollectionBase, int>();
            _cached_relation_tags = new List<Tuple<int, Tag>>();
            _unique_relation_tags = new Dictionary<TagsCollectionBase, int>();

            // node caches
            _cached_nodes = new List<Node>();

            // way caches
            _cached_ways = new List<Way>();
            _cached_way_nodes = new List<Tuple<long?, long?, long?>>();

            // relation caches
            _cached_relations = new List<Relation>();
            _cached_relation_members = new List<Tuple<long?, RelationMember, long?>>();
        }

        private SQLiteCommand CreateBatchCommand(SQLiteConnection connection,
            string sql_base, string table_name, int batch_count, params Tuple<string, DbType>[] parameters)
        {
            // fill out the parameters definition string
            var params_sb = new StringBuilder();
            // fill values list stub
            var values_stub_sb = new StringBuilder("(");
            for (var i = 0; i < parameters.Length; ++i)
            {
                params_sb.Append(parameters[i].Item1);
                values_stub_sb.Append(@"@" + parameters[i].Item1 + @"{0}");

                if (i != parameters.Length - 1)
                {
                    values_stub_sb.Append(@",");
                    params_sb.Append(@",");
                }
            }
            values_stub_sb.Append(")");

            // build the values list
            var values_sb = new StringBuilder();

            // fill out the values component of the sql query
            for (var i = 0; i < batch_count; ++i)
            {
                values_sb.Append(String.Format(values_stub_sb.ToString(), i));

                if (i != batch_count - 1)
                {
                    values_sb.Append(@",");
                }
            }

            // create the final sql command
            var sql = String.Format(sql_base, table_name, params_sb, values_sb);

            // create the command
            var command = new SQLiteCommand(sql, connection);

            // load parameters into the command
            for (var i = 0; i < batch_count; ++i)
            {
                foreach (var parameter in parameters)
                {
                    command.Parameters.Add(@"@" + parameter.Item1 + i, parameter.Item2);
                }
            }

            return command;
        }

        private SQLiteCommand CreateFlushCommand(SQLiteConnection connection, string flush,
            string table_name, params Tuple<string, DbType>[] parameters)
        {
            // fill out the parameters definition string
            var params_sb = new StringBuilder();
            // fill values list stub
            var values_sb = new StringBuilder("(");
            for (var i = 0; i < parameters.Length; ++i)
            {
                params_sb.Append(parameters[i].Item1);
                values_sb.Append(@"@" + parameters[i].Item1);
                if (i != parameters.Length - 1)
                {
                    values_sb.Append(@",");
                    params_sb.Append(@",");
                }
            }
            values_sb.Append(")");

            // create the final sql command
            var sql = String.Format(flush, table_name, params_sb, values_sb);

            // create the command
            var command = new SQLiteCommand(sql, connection);

            // load parameters into the command
            foreach (var parameter in parameters)
            {
                command.Parameters.Add(parameter.Item1, parameter.Item2);
            }

            return command;
        }

        private void ProcessTagsIdAndName(SQLiteCommand command, OsmGeo geo,
            Dictionary<TagsCollectionBase, int> _uniques, int params_count, int batch_index = 0)
        {
            // name & tags id
            if (geo.Tags != null)
            {
                // filter the geo tags
                var filtered_tags = _tag_filter.Evaluate(geo);

                // if we actually returned some tags
                if (filtered_tags.Count > 0)
                {
                    // we should have those filtered tags in the unique dict
                    if (_uniques.ContainsKey(filtered_tags))
                    {
                        command.Parameters[(batch_index * params_count) + params_count - 2].Value = _uniques[filtered_tags];

                        // we only add geo names if they are of interest via tags
                        if (geo.Tags.ContainsKey("name"))
                        {
                            var name = geo.Tags["name"];

                            if (!string.IsNullOrEmpty(name))
                            {
                                command.Parameters[(batch_index * params_count) + params_count - 1].Value = name.Truncate(500);
                            }
                            else
                            {
                                command.Parameters[(batch_index * params_count) + params_count - 1].Value = DBNull.Value;
                            }
                        }
                        else
                        {
                            command.Parameters[(batch_index * params_count) + params_count - 1].Value = DBNull.Value;
                        }
                    }
                    // if not, somethings wrong!
                    else
                    {
                        throw new InvalidOperationException(
                            geo + " passes filtered tags, but filtered tags are not in unique table.");
                    }
                }
                // no tags of interest, name and tag id are null
                else
                {
                    command.Parameters[(batch_index * params_count) + params_count - 2].Value = DBNull.Value;
                    command.Parameters[(batch_index * params_count) + params_count - 1].Value = DBNull.Value;
                }
            }
            // no tags at all, name and tag id are null
            else
            {
                command.Parameters[(batch_index * params_count) + params_count - 2].Value = DBNull.Value;
                command.Parameters[(batch_index * params_count) + params_count - 1].Value = DBNull.Value;
            }
        }

        private void AddTags(OsmGeo geo, Dictionary<TagsCollectionBase, int> uniques, List<Tuple<int, Tag>> cache)
        {
            if (geo.Tags != null)
            {
                var filtered_tags = _tag_filter.Evaluate(geo);

                if (filtered_tags.Count > 0)
                {
                    // ensure we haven't added this combination before
                    if (!uniques.ContainsKey(filtered_tags))
                    {
                        // add each tag to the tag cache
                        foreach (var tag in filtered_tags)
                        {
                            if (cache.Count == TagsBatchCount)
                            {
                                BatchAddTags(cache, geo.Type);
                            }

                            cache.Add(new Tuple<int, Tag>(uniques.Count, tag));
                        }

                        // add it to the hashset
                        uniques[filtered_tags] = uniques.Count;
                    }
                }
            }
        }

        private void AddWayNodes(Way way)
        {
            if (way.Nodes != null)
            {
                long? sequence_id = 0;
                foreach (var node in way.Nodes)
                {
                    if (_cached_way_nodes.Count == WayNodesBatchCount)
                    {
                        BatchAddWayNodes(_cached_way_nodes, _replaceWayNodesBatchCommand);
                    }

                    _cached_way_nodes.Add(new Tuple<long?, long?, long?>(way.Id, node, sequence_id));
                    ++sequence_id;
                }
            }
        }

        private void AddRelationMembers(Relation relation)
        {
            if (relation.Members != null)
            {
                long? sequence_id = 0;
                foreach (var relation_member in relation.Members)
                {
                    if (_cached_relation_members.Count == RelationMembersBatchCount)
                    {
                        BatchAddRelationMembers(_cached_relation_members, _replaceRelationMembersBatchCommand);
                    }

                    _cached_relation_members.Add(new Tuple<long?, RelationMember, long?>(relation.Id, relation_member, sequence_id));
                    ++sequence_id;
                }
            }
        }

        private void BatchAddNodes(List<Node> nodes, SQLiteCommand command)
        {
            var i = 0;
            foreach (var node in nodes)
            {
                // id
                command.Parameters[(i * NodeParamsCount) + 0].Value = node.Id.ConvertToDBValue<long>();

                // lat & lon
                int? latitude = SQLiteSchemaTools.GeoToDB((double)node.Latitude);
                command.Parameters[(i * NodeParamsCount) + 1].Value = latitude.ConvertToDBValue<int>();
                int? longitude = SQLiteSchemaTools.GeoToDB((double)node.Longitude);
                command.Parameters[(i * NodeParamsCount) + 2].Value = longitude.ConvertToDBValue<int>();

                // tile
                command.Parameters[(i * NodeParamsCount) + 3].Value = Tile.CreateAroundLocation(new Math.Geo.GeoCoordinate(node.Latitude.Value, node.Longitude.Value), SQLiteSchemaTools.DefaultTileZoomLevel).Id;

                // tags id and name
                ProcessTagsIdAndName(command, node, _unique_node_tags, NodeParamsCount, i);

                ++i;
            }

            command.ExecuteNonQuery();

            _node_count += nodes.Count;
            nodes.Clear();
        }

        private void FlushAddNodes(List<Node> nodes, SQLiteCommand command)
        {
            foreach (var node in nodes)
            {
                // id
                command.Parameters[0].Value = node.Id.ConvertToDBValue<long>();

                // lat & lon
                int? latitude = SQLiteSchemaTools.GeoToDB((double)node.Latitude);
                command.Parameters[1].Value = latitude.ConvertToDBValue<int>();
                int? longitude = SQLiteSchemaTools.GeoToDB((double)node.Longitude);
                command.Parameters[2].Value = longitude.ConvertToDBValue<int>();

                // tile
                command.Parameters[3].Value = Tile.CreateAroundLocation(new Math.Geo.GeoCoordinate(node.Latitude.Value, node.Longitude.Value), SQLiteSchemaTools.DefaultTileZoomLevel).Id;

                // tags id and name
                ProcessTagsIdAndName(command, node, _unique_node_tags, NodeParamsCount);

                command.ExecuteNonQuery();
            }

            _node_count += nodes.Count;
            nodes.Clear();
        }

        private void BatchAddWays(List<Way> ways, SQLiteCommand command)
        {
            var i = 0;
            foreach (var way in ways)
            {
                // id
                command.Parameters[(i * 3) + 0].Value = way.Id.ConvertToDBValue<long>();

                // tags id and name
                ProcessTagsIdAndName(command, way, _unique_way_tags, WayParamsCount, i);

                ++i;
            }

            command.ExecuteNonQuery();

            _total_way_count += ways.Count;
            ways.Clear();
        }

        private void FlushAddWays(List<Way> ways, SQLiteCommand command)
        {
            foreach (var way in ways)
            {
                // id
                command.Parameters[0].Value = way.Id.ConvertToDBValue<long>();

                // tags id and name
                ProcessTagsIdAndName(command, way, _unique_way_tags, WayParamsCount);

                command.ExecuteNonQuery();
            }

            _total_way_count += ways.Count;
            ways.Clear();
        }

        private void BatchAddWayNodes(List<Tuple<long?, long?, long?>> way_nodes, SQLiteCommand command
            )
        {
            var i = 0;
            foreach (var way_node in way_nodes)
            {
                command.Parameters[(i * 3) + 0].Value = way_node.Item1.ConvertToDBValue<long>();
                command.Parameters[(i * 3) + 1].Value = way_node.Item2.ConvertToDBValue<long>();
                command.Parameters[(i * 3) + 2].Value = way_node.Item3.ConvertToDBValue<long>();

                ++i;
            }

            command.ExecuteNonQuery();

            _total_way_nodes_count += way_nodes.Count;
            way_nodes.Clear();
        }

        private void FlushAddWayNodes(List<Tuple<long?, long?, long?>> way_nodes, SQLiteCommand command)
        {
            foreach (var way_node in way_nodes)
            {
                command.Parameters[0].Value = way_node.Item1.ConvertToDBValue<long>();
                command.Parameters[1].Value = way_node.Item2.ConvertToDBValue<long>();
                command.Parameters[2].Value = way_node.Item3.ConvertToDBValue<long>();

                command.ExecuteNonQuery();
            }

            _total_way_nodes_count += way_nodes.Count;
            way_nodes.Clear();
        }

        private void BatchAddRelations(List<Relation> relations, SQLiteCommand command)
        {
            var i = 0;
            foreach (var relation in relations)
            {
                // id
                command.Parameters[(i * 3) + 0].Value = relation.Id.ConvertToDBValue<long>();

                // tags id and name
                ProcessTagsIdAndName(command, relation, _unique_relation_tags, RelationParamsCount, i);

                ++i;
            }

            command.ExecuteNonQuery();

            _total_relation_count += relations.Count;
            relations.Clear();
        }

        private void FlushAddRelations(List<Relation> relations, SQLiteCommand command)
        {
            foreach (var relation in relations)
            {
                // id
                command.Parameters[0].Value = relation.Id.ConvertToDBValue<long>();

                // tags id and name
                ProcessTagsIdAndName(command, relation, _unique_relation_tags, RelationParamsCount);

                command.ExecuteNonQuery();
            }

            _total_relation_count += relations.Count;
            relations.Clear();
        }

        private void BatchAddRelationMembers(List<Tuple<long?, RelationMember, long?>> relation_members, SQLiteCommand command)
        {
            var i = 0;
            foreach (var relation_member in relation_members)
            {
                command.Parameters[(i * 5) + 0].Value = relation_member.Item1.ConvertToDBValue<long>();

                if (relation_member.Item2.MemberType == OsmGeoType.Node)
                {
                    command.Parameters[(i * 5) + 1].Value = 0;
                }
                else if (relation_member.Item2.MemberType == OsmGeoType.Way)
                {
                    command.Parameters[(i * 5) + 1].Value = 1;
                }
                else if (relation_member.Item2.MemberType == OsmGeoType.Relation)
                {
                    command.Parameters[(i * 5) + 1].Value = 2;
                }

                command.Parameters[(i * 5) + 2].Value = relation_member.Item2.MemberId.ConvertToDBValue<long>();
                var member_role = relation_member.Item2.MemberRole;
                command.Parameters[(i * 5) + 3].Value = member_role.Truncate(100);
                command.Parameters[(i * 5) + 4].Value = relation_member.Item3.ConvertToDBValue<long>();

                ++i;
            }

            command.ExecuteNonQuery();

            _total_relation_members_count += relation_members.Count;
            relation_members.Clear();
        }

        private void FlushAddRelationMembers(List<Tuple<long?, RelationMember, long?>> relation_members, SQLiteCommand command)
        {
            foreach (var relation_member in relation_members)
            {
                command.Parameters[0].Value = relation_member.Item1.ConvertToDBValue<long>();

                if (relation_member.Item2.MemberType == OsmGeoType.Node)
                {
                    command.Parameters[1].Value = 0;
                }
                else if (relation_member.Item2.MemberType == OsmGeoType.Way)
                {
                    command.Parameters[1].Value = 1;
                }
                else if (relation_member.Item2.MemberType == OsmGeoType.Relation)
                {
                    command.Parameters[1].Value = 2;
                }

                command.Parameters[2].Value = relation_member.Item2.MemberId.ConvertToDBValue<long>();
                var member_role = relation_member.Item2.MemberRole;
                command.Parameters[3].Value = member_role.Truncate(100);
                command.Parameters[4].Value = relation_member.Item3.ConvertToDBValue<long>();

                command.ExecuteNonQuery();
            }

            _total_relation_members_count += relation_members.Count;
            relation_members.Clear();
        }

        private void BatchAddTags(List<Tuple<int, Tag>> tags, OsmGeoType type)
        {
            var i = 0;

            foreach (var tag in tags)
            {
                var key = tag.Item2.Key;
                var value = tag.Item2.Value;

                _replaceTagBatchCommands[type].Parameters[(i * 3) + 0].Value = tag.Item1;
                _replaceTagBatchCommands[type].Parameters[(i * 3) + 1].Value = key.Truncate(100);
                _replaceTagBatchCommands[type].Parameters[(i * 3) + 2].Value = value.Truncate(500);

                ++i;
            }

            _replaceTagBatchCommands[type].ExecuteNonQuery();

            tags.Clear();
        }

        private void FlushAddTags(List<Tuple<int, Tag>> tags, OsmGeoType type)
        {
            foreach (var tag in tags)
            {
                var key = tag.Item2.Key;
                var value = tag.Item2.Value;

                _replaceTagFlushCommands[type].Parameters[0].Value = tag.Item1;
                _replaceTagFlushCommands[type].Parameters[1].Value = key.Truncate(100);
                _replaceTagFlushCommands[type].Parameters[2].Value = value.Truncate(500);

                _replaceTagFlushCommands[type].ExecuteNonQuery();
            }

            tags.Clear();
        }

        private void DisposeCommandDictionary(Dictionary<OsmGeoType, SQLiteCommand> commands)
        {
            foreach (var command in commands)
            {
                command.Value.Dispose();
            }
        }
    }
}
